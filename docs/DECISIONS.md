# Decisions

## 一期規劃

一期目標：用 Rust + PyO3 輸出與 mftiko 相容的 STS Python SDK，驗收為 `mftiko-conformance` 全部 pass（mftiko PROTOCOL §9）。票做到此 epic 時再拆。

先前定案的一期規劃：Rust 核心 + PyO3、crate `mftik-proto`／`mftik-core`／`mftik-py`、snake_case API 與 Go SDK 一對一、相容標準為 mftiko PROTOCOL §9 與 `mftiko-conformance`。

### E01 專案基礎

範圍：清空 repo、Cargo workspace（`mftik-proto`、`mftik-core`、`mftik-py`）、`docs/DECISIONS.md`（E01-D001 起）、從 mftiko 複製並釘住 `contract/proto` 加同步腳本、CI（cargo test/clippy/fmt、maturin build、Python 冒煙測試）。

依賴：無。

### E02 核心演算法（純 Rust）

範圍：`book`（tick 索引訂單簿，以 Go 版測試與 benchmark 對照）、`bookpath`（本地簿同步：斷號／conn_epoch、快照退避 1–30s、增量緩衝 10000 筆或 10s）、`dispatch`（MD ring 32 MiB、TD FIFO 65536、TD 優先、prehook、hook 時間預算）、`tdpath`（OMS、Ledger、Recon 換 OMS）、`cloidgen`、`volume` 原子寫入。不碰 IO，放 `mftik-core`。

依賴：E01。

### E03 Runtime（C4 / C6 / SYM）

範圍：C4 fd 3（frame、Hello/Init/Start/Stop、Rust 執行緒送 Beat、Report、結束順序與 exit code）、C6 NATS（連線、binding、`md.f` envelope、`md.view`/`md.fetch`、下單撤單、`td.evt`/`td.view`）、`symclient`。

依賴：E02（至少 dispatch）。

### E04 Python SDK（PyO3）

範圍：PyO3 綁定（策略基底類別、snake_case hook 與 Go SDK 一對一、讀取介面、hook 例外視為 CRASHED）；打包（maturin abi3 wheel，Python 3.11+；產生 OCI artifact 的腳本，內含 python-build-standalone）。

依賴：E03。

### E05 驗收

範圍：Python 版 conformance 標準策略，`mftiko-conformance` 全部 pass 並在每個 PR 的 CI 跑；範例策略與使用文件；在實際 mftiko 環境部署一次 Python 策略跑完 Init/Start/Stop。

依賴：E04。

## 決定（2026-10-25，E01-D001–E01-D007）
- E01-D001 清空前在最後的舊 commit 打 `legacy-final` tag。
- E01-D002 刪除 `release.yml`、`cleanup.yml`、`tests.yml`；Strategon 上的部署與 PyPI 舊版不動。
- E01-D003 套件名沿用 `mftik`（import `mftik`），版本從 0.12.0 起，E04 才發佈。
- E01-D004 整包複製 mftiko `contract/proto/mftiko`，釘在 BullionBear/mftiko `e7b3543e2269c4ae8c10631edd448bdb86c52e2c`，同步腳本記錄來源 commit。
- E01-D005 `build.rs` + prost-build，protoc 用 `protoc-bin-vendored`，產生的程式碼不進 git。
- E01-D006 `rust-toolchain.toml` 釘目前 stable、edition 2024；Python 用 uv + maturin，lint 用 ruff；CI 在 GitHub Actions ubuntu，clippy 警告即失敗。
- E01-D007 CI 先備好以 `go install` 從釘住的 mftiko commit 編 `mftiko-conformance` 的步驟，E05 才接上執行。
- 也包含先前定案的一期規劃：Rust 核心 + PyO3、crate `mftik-proto`／`mftik-core`／`mftik-py`、snake_case API 與 Go SDK 一對一、相容標準為 mftiko PROTOCOL §9 與 `mftiko-conformance`。

## 決定（2026-10-25，E02-D001–E02-D005）

Yi Te 於 #394 確認。與 Go 版語意不同之處先回報再做。E02 模組是純 Rust、無 IO，放在 `mftik-core`。

Yi Te 在 E02 票上確認的共同決定。E02-03 沒有另開 E02-D031 以後的決定。

- E02-D001 sans-IO：E02 模組只做純狀態機，輸入事件與現在時間（由外部注入），輸出要做的動作（如請求 md.view 快照、設計時器、交給哪個 hook）。NATS、計時器等 IO 留給 E03。測試用假時鐘，不 sleep。
- E02-D002 執行緒：dispatcher 跑在一條專用 OS 執行緒（之後也在此呼叫 Python hook）；TD FIFO 與 MD ring 用 Mutex+Condvar，不做 lock-free；tokio 只用於 E03。hook 超時由 watchdog 執行緒送 Exiting(HUNG) 後 exit 21。
- E02-D003 數值：價格與數量用 f64，tick 用 i64，Tick() 四捨五入、0.5 遠離零，與 mftiko MD conn worker 一致。
- E02-D004 驗證：不搬 Go 測試、不逐一比對 Go 行為，只要求語意一致（以 mftiko PROTOCOL §9 與 Go SDK 為語意參考）；寫的過程中補自己的 Rust 測試驗證。book 附 criterion benchmark 數字，不與 Go 對照、無硬門檻。
- E02-D005 並行：E02-01/03/04/05 同時開工，E02-02 等 E02-01 合併。各票決策區段：E02-01 D011 起、E02-02 D021 起、E02-03 D031 起、E02-04 D041 起、E02-05 D051 起。後合併的 PR 以 merge main 解 DECISIONS 衝突，不 rebase。
- 與 Go 版語意不同之處，先回報再做。純 Rust、無 IO，放在 mftik-core。

E02-03 把 E02-D001 用在 dispatch 上的方式（不是新決定）：硬預算、軟限制、prehook 都讀注入的 `now_ns`，不讀牆鐘。Go SDK 的硬逾時是 `time.NewTimer`；這裡由 host 在 hook 執行期間持續呼叫 `set_now_ns`，watchdog 執行緒在注入時間用盡時發布 `RunEnd::Hung`。送 `Exiting` 與 exit code 是 E03 的 IO。`Timer::after` 只回報 deadline 並請 host 排程，host 到期後呼叫 `Timer::fire`。

## 決定（2026-10-25，E02-D021–E02-D029）

E02-02 本地簿。語意對齊 mftiko PROTOCOL §9.5（E08-D057–E08-D061）與 `sdk/internal/bookpath`。緩衝 10000 筆或 10 秒、退避 1 秒起加倍到 30 秒，沿用 E04-D024。環長是 `book::DEFAULT_N`。檔位交給 `OrderBook::apply`。

- E02-D021 介面：`bookpath::Path` 吃事件與該次呼叫的 `now_ns`，吐 `RequestSnapshot`、`ArmTimer`、`Hook`、`Log`。`resolve`／`forget` 寫入這個 session 已解析的 `price_tick`。`on_delta` 吃已解開的 `orderbook_delta`。`on_message` 吃 dispatcher book callback 的 `md.f` bytes。`on_non_delta` 吃不是 `orderbook_delta` 的事件。`on_snapshot` 吃 `md.view` 的回覆。`on_timer` 把 `ArmTimer` 的 token 原樣交回。沒有 callback，不讀牆鐘，不 sleep。NATS 與計時器是 E03。
- E02-D022 時鐘：一則呼叫用該次傳入的 `now_ns`。`OnBook` 的條件是 `now_ns - md_ts_ns <= recv_window`（含等於），用 i128，對齊協定的比較，不沿用 Go `time.Duration` 溢位。緩衝從這一次嘗試的第一筆起算，年齡 `>= 10s` 就丟。`ArmTimer` 帶 `after` 與 `deadline_ns`。緩衝計時器在注入時間還沒到就響，這次忽略、不重排；下一筆 delta 仍用年齡判斷。退避計時器響了不重讀時鐘。
- E02-D023 商品閘門：不呼叫 SYM。UID 不在表內只記一次 `book: uid N is not resolved`。tick 不是有限且大於 0，只記一次 `book: uid N has no price tick`。成功裝上快照才清除「記過」。閘門失敗時，已經同步的簿留著，之後的 delta 直接丟，直到 tick 又可用。新的 tick 等下一次建簿才用。
- E02-D024 快照：`RequestSnapshot` 帶 `uid`、`generation` 與 `wait`（等於 `recv_window`）。`generation` 從 0 起，只有作廢才加一，失敗重試沿用同一個，所以它是過期記號，不是全域唯一的請求編號。同一商品同時只有一張快照在途。host 用請求時的 `(uid, generation)` 回一次。世代不符，或已經不在途，回覆丟掉。`SnapshotReply::Empty` 與空的 `Failed` 記 `bookpath: empty snapshot`。回覆 uid 非 0 且不同，記 `bookpath: snapshot uid N`。游標早於 floor，或緩衝接不上去，記 `bookpath: snapshot is behind the feed`，然後退避。失敗文字由 host 傳入，原樣記在 `book: uid N: ...`。
- E02-D025 計時與緩衝：每個 `ArmTimer` 有自己的 `id`。host 每個 token 最多觸發一次；同種類只認最後一次 arm 的 id。`BufferAge` 的 `generation` 是 `buf_gen`，`Backoff` 的是快照 `gen`。已在退避或已有快照在途，不另起一次讀取。緩衝到 10000 筆或 10 秒：丟掉緩衝、floor 抬到緩衝裡最大的 `(conn_epoch, seq)`、作廢在途快照與退避、立刻再要。成功把退避歸零。下一筆失敗從 1 秒再起。16 秒再加倍是 30 秒，之後停在 30 秒。
- E02-D026 套用：未同步的增量只進緩衝，不是 hook。快照之後只套游標更大、而且與游標連續的；緩衝裡套上的仍不是 hook。比游標舊的或重複的丟掉。連續用 `uint64` 加法，和 Go 一樣繞回；「比游標新」仍是字典序，`seq` 必須大於游標。游標已是最大值時，seq 0 不算更新，整則丟掉。斷號或 `conn_epoch` 改變就未同步，那筆增量留在緩衝，floor 抬到它，再要快照。`reset` 清簿、標未同步、不套這則的檔位，floor 至少到這則游標；比已同步游標、floor、或緩衝最大游標舊的 `reset` 忽略。`view` 在未同步時是 `None`。處理 `Hook` 時 `view` 已經含這一則。太舊的 delta 仍套用，只是不發 `Hook`。
- E02-D027 位元組：`on_message` 用 dispatcher 的 v1 envelope（不另寫 parser），digest 不查，再解 `FeedEvent`。壞 envelope 記 `book: envelope: ...`，不動任何游標。壞 payload 記 `book: payload: ...` 後返回；錯誤文字是 prost 的，與 Go `proto.Unmarshal` 的文字不同，游標同樣不動，下一則同商品的 delta 會因跳號重同步。uid 0 只記一次 `book: uid 0`。不是 `orderbook_delta` 的事件走斷號：未同步、floor 抬到該游標、立刻再要快照，不進退避。
- E02-D028 prehook：bookpath 不因年齡拒收 delta。`push_book` 已經繞過 stale prehook，而且這個年齡不計入 `QueueStat.dropped`。host 把 book callback 的 bytes 交給 `on_message`。壞 envelope 由 dispatcher 先拒絕並計入 `dropped`，那些 bytes 到不了 bookpath。
- E02-D029 `close` 之後 `view` 是 `None`。之後的 delta、快照回覆、計時器都忽略。不送取消；在途 `md.view` 的回覆丟掉。

## 決定（2026-10-25，E03-D001–E03-D008）

Yi Te 於 #385 確認。E03 的共同決定，全文也附在 #404–#413。

- E03-D001 sans-IO 延伸：mdpath、C4 狀態機、symclient 的協定邏輯延續 E02-D001，做成純狀態機放 `mftik-core`。fd 3、NATS、OS 計時器、檔案系統放 `mftik-rt`。
- E03-D002 crate：新增 `crates/mftik-rt`（tokio、async-nats）。`mftik-py` 之後只做 PyO3 綁定，E03 不碰 PyO3。
- E03-D003 執行緒：dispatcher 跑在呼叫 `run` 的執行緒（Python 時是主執行緒）。tokio 用 current_thread runtime，獨占一條 IO 執行緒。Beat 是一條 std 執行緒，不經 tokio、不經 dispatcher。watchdog 沿用 E02。fd 3 的寫入共用一把寫鎖，一則 frame 寫完才放開。
- E03-D004 時鐘：注入的 `now_ns` 是啟動時取一次 CLOCK_REALTIME，加上 CLOCK_MONOTONIC 的經過時間。dispatcher 在 hook 執行期間持續呼叫 `set_now_ns`。session 中途牆鐘被 step 時，prehook 與 OnBook 算出的年齡會差這一段；hook 預算與退避不受影響。這是與 Go（prehook 用牆鐘、時長用單調鐘）的已知差異，E02 的 API 不改。
- E03-D005 結束：每種結束都先 flush fd 3，再 `libc::_exit(code)`，不跑 atexit 與 finalizer。對齊 Go 的 `os.Exit` 不跑 defer；watchdog 執行緒在別人持有 GIL 時呼叫 `exit` 會卡在 Python 的 finalize。
- E03-D006 NATS：async-nats。認證用 `user_jwt` 加 nkey `seed` 簽章，`custom_inbox_prefix` 設成 `inbox_prefix`，位址是 `MFTIKO_NATS_URL`（§9.4.1、E09-D006）。
- E03-D007 驗收：E03 以 Rust 版 conformance 標準策略跑 `mftiko-conformance`，每一項都要 pass。CI 的 conformance job 在 E03-10 打開。這取代 E01-D007 的「E05 才接上執行」；E05 改用 Python 版策略。
- E03-D008 並行與決策區段：E03-01、02、03、04 同時開工；E03-05 等 E03-02、E03-03；E03-06、07、08 等 E03-05；E03-09 等 E03-06、E03-07；E03-10 最後。各票的決策區段：E03-01 從 D011 起，E03-02 從 D021 起，依此類推，E03-10 從 D101 起。後合併的 PR 以 merge main 解 DECISIONS 衝突，不 rebase。
- 與 Go 版語意不同之處，先回報再做。

## 決定（2026-10-25，E03-D011–E03-D014）

E03-01 mdpath。語意對齊 mftiko PROTOCOL §7.1、§9.2.9、§9.3.2、§9.3.5、§9.4.2–§9.4.4（E08-D005、E08-D046–E08-D049）與 `sdk/internal/mdpath`。

- E03-D011 套用 binding：`mdpath::Path::apply` 吃 `stsctl.Bindings`，先擋 selector（不看版本），再驗 feed（`topic.ticker`、subject 非空、一個 key 一個 subject、一個 subject 一個 key），然後比版本。版本沒有比較新回 `Apply::Unchanged`，算成功。比較新回 `Apply::Staged`，內含照 subject 排序的 `Subscribe`。host 訂閱、有新訂閱就 flush，成功呼叫 `apply_bound`：這時才換集合、記 `bindings_applied`，回 `Unsubscribe`；失敗呼叫 `apply_aborted`：退訂這次新開的，集合與版本不變。這是 Go `Apply` 的順序（先訂、flush、換集合、再退訂，flush 失敗不算套用），拆成兩步讓 host 做 IO。`apply_bound` 或 `apply_aborted` 之前再 `apply` 回 `ApplyInProgress`；Go 以 mutex 排隊，這裡由 host 排隊。`FeedBinding.state`、`md_instance`、`accounts` 不讀，與 Go 相同。
- E03-D012 訂閱與路由：每個訂閱有自己的 `SubId`。同一個 subject 上 feed key 改了，是新開一個訂閱、退訂舊的，兩個 id 不同。`route(sub, bytes)` 在 IO 執行緒用：不在集合裡 `Skip`；envelope 壞掉 `Md`，交給 dispatcher 計入 `dropped` 並記 log；`orderbook_delta` 是 `Book`，走 `push_book` 給 bookpath；沒有 hook 的 topic `Skip`，不解 payload；其他 `Md`。staged 的新訂閱一開始就收；要退訂的到 `apply_bound` 才停，與 Go 的 `live` 旗標相同。`Hooks::decode` 吃 dispatcher feed callback 的 bytes（已過 envelope 與 prehook），重讀 envelope、解 `FeedEvent`，回 `Hook`、`Skip`（`orderbook_delta`、空 oneof、該 topic 沒有 hook）或 `Log`。payload 解不開記 `md payload rejected: ...`，不交付、不算 envelope drop；文字是 prost 的（同 E02-D027）。
- E03-D013 讀取：`tape`、`book_snapshot` 的 ticker，`fetch_klines`、`fetch_open_interest` 的 venue，都由呼叫端從 SYM `Info` 傳入，mdpath 不查 SYM，也不另存商品表。ticker 空字串回 `UnknownUid`。讀的是已 bound 的集合。回 `Request`：ticket、subject、編好的 body、`wait = recv_window`。host 以 ticket 呼叫 `on_reply`，交回回覆 bytes、`Timeout`、`NoResponder` 或其他失敗，拿到 `Delivery` 推上 TD buffer（E08-D048）。錯誤文字照 Go：`mdpath: md.view: ...`；逾時是 `context deadline exceeded`，沒有 responder 是 `nats: no responders available for request`；`md.fetch` 有 reason 時是 `error: reason`；沒有結果是 `empty tape`、`empty book`、`empty klines`、`empty open interest`。不認得或已回過的 ticket 回 `None`。
- E03-D014 `close`：退訂集合與 staged 的新訂閱，丟掉在途讀取，之後的回覆回 `None`。之後 `apply`、`apply_bound`、讀取回 `Stopping`；selector、壞 feed、壞參數仍先回，順序同 Go。`route` 一律 `Skip`。`bindings_applied` 保留最後一份。Go 在 dispatcher 結束後才 `Close`，那時 TD buffer 已關，在途回覆同樣交不出去。

## 決定（2026-10-25，E03-D021–E03-D027）

E03-02 C4 的 worker 端。語意對齊 mftiko `contract/wire/c4` 與 `sdk/run.go` 的 session 設定（PROTOCOL §5.2、§5.3、§9.2，E08-D003、E08-D006–E08-D009、E08-D037）。

- E03-D021 介面：`c4::Machine` 吃 `hello`、`on_down`（解好的 `WorkerDown`）、`on_eof`、`on_sigterm`、`on_timer`，以及 plane 呼叫的結果 `init_returned`、`start_returned`、`run_returned`、`live_returned`、`on_stop_returned`，每次帶注入的 `now_ns`；吐 `Effect`：`Send`、`StartBeat`／`StopBeat`、`CallInit`／`CallStart`／`CallRun`／`CallLive`／`CallRelay`／`CallOnStop`、`Cancel`、`ArmTimer`、`Log`、`Exit`。`Config::session` 是 kind `session`、contract `stsctl.v1`、contracts `worker.v1`、`stsctl.v1`、`md.v1`、`td.v1`、`init_carries_live`；pid 由 host 傳入。session plane 的 Relay 回聲（`sdk/plane.go`）併進狀態機：Init 成功後的 `Relay` 先原樣送回 corr 與 payload，再交給 plane。`dispatch::RunEnd` 轉 `RunResult`：`Done` 是正常返回、`Failed` 是 `Exit(FAILED)`、`Crashed` 是錯誤、`Hung` 是 `Exit(HUNG, hook timeout)`；`StopError` 的 `Deadline`、`Cancelled` 轉 `CallError::Cancelled`。
- E03-D022 計時：Init、Start 的 `deadline_ms` 與自行結束時 OnStop 的 hook 預算，由狀態機以 `ArmTimer` 請 host 排；token 對得上就照辦，不重讀時鐘。plane 呼叫因 context 結束而返回時回 `CallError::Cancelled`（Go 的 `ctx.Err()`）：Init、Start 在注入時間已過 deadline 時算 `deadline`；OnStop 已過 hook 預算時 exit 21、不送 `Stopped`（Go 的 `timeoutResult`、`hookHung`）。結果與計時器誰先到算誰，晚到的忽略。讀到 Stop、EOF 或 SIGTERM 之後 deadline 不再生效，等 hook 返回（Go 的 `runLimited`）。grace 的期限只交給 OnStop，狀態機照樣等它，grace 到期由 shim 結束進程（E08-D009）。
- E03-D023 Beat：`Hello` 之後 `StartBeat`；第一則 Stop、EOF 或 SIGTERM 時 `StopBeat`，對齊 Go 的 beat goroutine 跟著 `life` 結束。deadline 與自行結束不停 Beat，由 `Exit` 一起結束。
- E03-D024 寫入失敗：`Send` 帶 `fatal`。`Hello`、Init 的 `Applied`、`Done(INIT, ok)`、`Done(START, ok)`、`Stage(running)` 寫不出去，host 以 exit 1 結束，後面的 effect 不做（Go 回 `ExitOther`）；其他寫入錯誤忽略。
- E03-D025 順序：一次只套用一則 Live，依到達順序。Init 成功之前收到的 Live 在 `Done(INIT, ok)` 之前套用完，之後才到的排在 `Done(INIT, ok)` 之後。Init 進行中收到的 `Start` 或第二則 `Init` 只記第一則，Init 成功後才處理；同時已讀到 Stop 時 Stop 優先（Go 在這裡隨機），以 DONE 結束，不呼叫 OnStart 與 OnStop。Start 接下之後的 `Init`、`Start` 忽略（Go 不再讀）。`Exiting` 送出之後 Live、Relay 照樣交給 plane（Go 的讀取 goroutine 讀到 `Serve` 返回為止）。Init 成功之前的 Stop、EOF、SIGTERM 以 DONE、exit 0 結束；Live 先留著；Relay 丟掉；`Controller` 任何時候都記下；`Undeliverable` 忽略（stsctl 沒有 worker 對 agent 的請求，E09-D003）。這幾點照 Go，與 PROTOCOL §5.2 E08-D037 的字面不同，列在 E03-02 PR 的「需要決定」。
- E03-D026 文字：reason 與 log 對齊 Go：`c4: start before init`、`c4: init after init`、`contract X`、`deadline`、`signal`（SIGTERM，或 reason 空的 Stop）、`shim closed`（EOF）；`CallError::Cancelled` 的文字是 `context canceled`。`Exiting` 的 reason 取第一則 Stop 或 EOF 的；SIGTERM 不記 reason，之後的 Stop 仍可設定。`init_carries_live` 時 `live_digest` 是空的也送 `Applied("")`，對齊 Go。
- E03-D027 EOF 之後的 OnStop（Yi Te 於 #415 確認，與 Go 不同，照 PROTOCOL §9.5.5）：fd 3 讀到 EOF 之後，沒有人能從外面結束這個進程，所以 OnStop 一律有上限，超過就 exit 21、不送 `Stopped`，`Exiting` 不改。讀過 `Stop` 的用那則 grace 的剩餘（shim 在時由 shim 執行，shim 不在由 worker 自己執行）；沒有的用 `hook_timeout_s`。SIGTERM 或 `Stop` 之後 OnStop 已在跑、還沒有上限時才讀到 EOF，就在讀到時補上同一個上限。`hook_timeout_s` 為 0 時不限時。Go 的 c4 在 EOF 之後不限時。

## 決定（2026-10-25，E03-D031–E03-D033）

E03-03 `mftik-rt` 的 fd 3 傳輸與 Beat。語意對齊 mftiko `contract/wire/frame`、`sdk.Run` 的 `ctlFD`、`contract/wire/c4` 的 beat（PROTOCOL §5.1、§9.2.1、§9.2.3）。C4 協定邏輯不在這裡（E03-D001）。

- E03-D031 控制 fd：`MFTIKO_CTL_FD` 未設或空字串是 3；其餘照 Go `strconv.Atoi` 的語法（十進位、可帶正負號）。負數或解析失敗回 `CtlFdError::Env`，exit 10，對齊 `sdk.Run`。超出 i32 也是 exit 10；Go 的 `int` 是 64 位元，這段 Go 會接受（kernel 再取低 32 位），只差在實際不會出現的值。拿到 fd 立刻 `fcntl(F_SETFD, FD_CLOEXEC)`；失敗（fd 沒開）回 `CtlFdError::Fd`，exit 1。Go 的 `syscall.CloseOnExec` 不回報錯誤、`os.NewFile` 不檢查，之後 `Hello` 寫不出去以 `ExitOther`（1）結束，所以兩邊都是 exit 1、不送任何 frame。`CtlFd::from_env` 是 `unsafe`：它取得 fd 的所有權，呼叫端保證只呼叫一次、process 裡沒有別人擁有這個 fd。
- E03-D032 frame：4 bytes big-endian 長度加 body，body 上限 1 MiB。寫入超過上限回 `TooLarge`，什麼都不寫；讀到 header 宣告超過上限回 `TooLarge`，不讀 body。frame 之間結束是 `Ok(None)`，header 或 body 中途結束是 `Truncated`（帶位置與位元組數）。body 解不開是 `Decode`，該 frame 已讀完、串流仍對齊。讀執行緒照 Go c4 的 reader：任何錯誤（含 `Decode`）回報一次就停，EOF 與錯誤由呼叫端都當成 shim 不在（§9.2.7）。未知欄位跳過，未知的 `WorkerDown` 種類以 `msg: None` 交出（§9.3）。`FrameWriter` 一把鎖涵蓋 encode 與整則寫出（E03-D003）。fd 3 用 `send(MSG_NOSIGNAL)` 寫，shim 消失時是 `EPIPE` 而不是 `SIGPIPE`，與 Go 對非 stdout fd 的行為相同；fd 不是 socket 時退回 `write(2)`。`seal` 先 flush，之後的 frame 一律回 `Sealed`，給 E03-D005 在 `_exit` 前用，避免 Beat 在 flush 之後才開始一則、被 exit 截斷。
- E03-D033 Beat：一條 std 執行緒，只等 fd 3 的寫鎖，不等其他執行緒。照 Go 的 ticker：啟動後一個週期（1 秒）送第一則，`seq` 從 1 起、每送一則加一，`progress` 留空。寫入卡住跨過週期時，錯過的週期合併成一則立刻送，之後回到原本的格點。`stop` 之後不再開始新的一則；寫入失敗（含 `Sealed`）就結束，不重試。呼叫端在 `Hello` 之後啟動它（Go 在 `Hello` 送出後才起 beat goroutine）。

## 決定（2026-10-25，E03-D041–E03-D045）

E03-04 SYM client。語意對齊 mftiko PROTOCOL §7.3、§9.4 第 8 條（E08-D004、E08-D022–E08-D026、E04-D057）與 `sdk/internal/symclient`。`symclient` 與 `symbol` 放 `mftik-core`，NATS、計時器、resolve 的阻塞等待留給 host（E03-D001）。

- E03-D041 介面：`symclient::Client` 吃事件與該次呼叫的 `now_ns`，吐 `Request`、`Subscribe`、`Unsubscribe`、`ArmTimer`、`Stored`、`Resolved`、`Log`。`Request` 帶 `Ticket`、`sym.v1.Request`、`wait` 與 `resolve`。`wait` 是 5 秒，Go 的預設；`resolve` 有值表示那個 resolve 正卡在這個請求上，hook 有期限時 host 改等到期限，與 Go 用 hook 的 ctx 相同。回覆以 `on_reply(ticket, Outcome)` 交回：`Body` 是收到的原始 bytes，natsx error reply（0x00 開頭）在 core 解，code 2 且訊息是 `not_found` 或以 `not_found:` 開頭才算查不到；`Failed` 是逾時、沒有 responder 或送不出去，文字由 host 給。`on_subscribed`、`on_changed(subject, bytes)`、`on_reconnect`、`on_timer` 同一個模式。`info(uid)` 只讀記憶體，不產生 effect。`close` 讓在途的 resolve 以 `Closed` 結束、每個發過 `Subscribe` 的 subject 都 `Unsubscribe`，之後的回覆、通知、計時器都忽略，`info` 仍可讀。
- E03-D042 resolve：host 只在 `OnInit`、`OnStart` 期間 `allow_resolve(true)`。窗外回 `NotInInit`，ticker 不合 strict parse 回 `InvalidTicker`，close 之後回 `Closed`，三者都同步、不送任何東西。之後照 Go 的順序：`GetRequest`（不設 `include_inactive`）；該表第一次時 `Subscribe`，等 `on_subscribed`，失敗則 resolve 失敗、列不存，下一次 resolve 再訂；存列並發 `Stored`；`Revisions` 與已確認的 revision 比，相同就結束，否則跑一輪重拿；最後 `Resolved` 帶當時的列。比對或重拿失敗只記 log，resolve 仍成功。SYM 查不到是 `NotFound`，不追蹤；策略回傳它就以 `INIT_FAILED` 結束。
- E03-D043 重拿：單位是 `(venue, category)`，同一張表同時只有一個比對或重拿。一輪是 `Revisions`、依 UID 順序對每個追蹤的列 `GetByUidRequest`、再 `Revisions`。兩次相同、而且這輪中間沒有 resolve 加進這輪沒列到的列，才確認 revision；否則重跑，最多 8 輪。活躍表 `not_found` 改以 `include_inactive` 再問；仍失敗，或回來的 UID、表不對，這輪失敗，已存的列保留，確認的 revision 不動。通知的 revision 大於確認值才重拿；解不開、subject 不符、revision 小於 1，改做全表比對（Go 的 `Reconnect`）。重拿進行中收到的通知記成 `want`，比對發現落後記成 `again`，結束後若仍需要再跑一輪。全表比對同時只有一個，期間再要一次就在結束後再跑一次。與 Go 不同處：Go 以 `loadMu` 把 resolve 與重拿排成一條；這裡 resolve 的 `GetRequest` 立刻送出，不排在背景重拿之後，不同表的重拿可以並行，以「新列使這輪不能確認」保住同一個不變式。
- E03-D044 計時與 log：約 5 分鐘的比對是 `ArmTimer`，第一筆列存入時才 arm，每次觸發後從該次 `now_ns` 再 arm（Go 是從 `Open` 起的固定 ticker）。只認最後一個 token。沒有追蹤任何表時，計時器、重連、壞通知都不送請求。log 沿用 Go 的文字：`symclient: sync {venue} {category}: …`、`symclient: reload {venue} {category}: …`、`symclient: revision check: …`；比對時每張失敗的表各記一行，不像 Go 併成一則。
- E03-D045 `Info` 與 bookpath：`symbol::Info` 對應 `contract/symbol.Info`，欄位一對一。proto 沒設的 optional 數字是 0、字串是空；filter 是 `BTreeMap<String, f64>`，有名字沒有界限的是 NaN（E00-D008）。不另加不活躍欄位，終結的 `Status` 就是標記（E08-D026）。`price_tick()` 回 `f64`：沒有這個 filter 是 0，沒有界限是 NaN，與 Go 的 `Filters[PriceTick]` 相同，bookpath 兩者都拒絕。每個 `Stored{uid}`，host 以 `bookpath::Path::resolve(uid, info.price_tick())` 更新 tick（E02-D023）。`symbol` 只移植 SDK 用到的：strict parse（含 strike 正規化檢查）、`Category`、`Status`、`valid_uid`、filter 名稱、margin 分組；寬鬆的 `Resolve`、`Listing`、資產別名表、`ExchKey` 不移植。大寫逐字元轉換：Go 用 simple case mapping，std 只有 full mapping，大寫是多個字元的字（如 `ß`）保持不變，與 Go 相同；少數帶 ypogegrammeni 的希臘字母與 Go 不同。

## 決定（2026-10-25，E03-D051–E03-D057）

E03-05 `mftik-rt` 的 runtime 骨架。語意對齊 mftiko `sdk/run.go`、`plane.go`、`sessctx.go`、`context.go`、`strategy.go`、`hooks.go`、`nats.go`（PROTOCOL §5、§9.2–§9.5，E08-D003、E08-D006、E08-D007、E08-D050）。MD、TD、SYM 的接線是 E03-06–E03-08，Report 是 E03-09。

- E03-D051 執行緒：呼叫 `run` 的執行緒執行 C4 要的 plane 呼叫：Init、Start、Run 的 dispatcher 迴圈、Run 開始前的 OnStop，所以每個策略 hook（含 OnInit、OnStart）都在這條執行緒（E03-D003，§9.9）。為此 `dispatch` 加 `Dispatcher::drive`（在呼叫端跑同一個迴圈，只另起 watchdog）與 `Dispatcher::wait_run`（等結果、不啟動迴圈）；`run` 不變。`mftik-c4` 執行緒擁有 `c4::Machine`，fd 3 frame、SIGTERM（signal-hook 的 `mftik-signal`）、計時器、plane 結果都經同一個 channel 依序交給它，它不等 hook。Run 的結果與 Run 之後的 OnStop 各由一條短命執行緒等 `wait_run`、`on_stop`。Cancel 結束目前 Init／Start 的 context；Run 中則 `Dispatcher::exit`（Go `Disp.Run` 的 ctx 取消）。每種結束都走 `exit::exit_with`：seal fd 3、flush stdout／stderr、`_exit`（E03-D005），含控制 fd 拿不到、執行緒起不來或 panic。
- E03-D052 時鐘：`Clock` 照 E03-D004。IO 執行緒每 10 ms 呼叫一次 `set_now_ns`；排入 hook 之前（dispatcher 計時器觸發、`Post`）先更新一次，hook 的起點與 MD 年齡因此是當下的，10 ms 只影響 watchdog 看到預算用完的延遲與閒置時 MD 年齡的誤差。E03-06、E03-07 的 push 路徑照做。1 ms 時閒置 worker 約吃 7%（release）一顆核，10 ms 約 1%。計時器只有一張表（`timers::Timers`，在 IO 執行緒）：C4、dispatcher `Timer`、bookpath、tdpath 退避、symclient 各有 `TimerKey`，同一個 key 再 arm 取代前一次，deadline 一次換成單調鐘的時刻。
- E03-D053 策略介面：`Strategy` 有 `on_init`、`on_start`（回 `Result<(), HookError>`，文字成為 `Done.error`）、`on_stop`。選用 hook 各一個 trait（`TradeHandler` 等，對應 Go 的 handler interface）；Rust 沒有型別斷言，策略在 `Strategy::handlers` 註冊，註冊要求實作該 trait，沒註冊的 topic 在解碼前就跳過（E08-D005）。`Context` 每個 hook 一個、不能 clone，只活到 hook 結束；跨執行緒唯一入口是 `Context::poster()` 的 `Poster::post`（Go `Post`）。callback 型別是 `FnOnce(&mut S, &Context<S>, ...)`：多了 `&mut S`，因為 Rust 的 closure 拿不到策略本身。讀取的 callback 收 `Result<T, ReadError>`。`book(uid)` 回 `Option<&OrderBook>`，借自該 hook 的 Context，所以帶不出 hook（E08-D003）。錯誤 `Error` 的文字同 Go 的 `sdk.Err*`，另有 `NotWired("md"|"td"|"sym")` 給還沒接線的路徑。volume 是 `mftik_rt::volume::{write, read}`，與 Go 的 `sdk/volume` 一樣由策略自己呼叫。`input_dir` 照 Go `relativeName` 檢查名字，目錄本身不再 Clean。
- E03-D054 Init／Start：`InitPayload` 的檢查順序與文字同 Go：`init payload`、`session id`、`recv window`、`hook timeout`（0 就是 `INIT_FAILED`，§9.3.1）、`nats credentials`、`nats url`。Start 解 `StartPayload`（沒有 bindings 是 `start payload`）、套 bindings、listen TD、印 `{"hook":"gate"}`、OnStart、第一次 Recon；策略已 exit／fail 時直接成功。OnInit、OnStart 的 panic 以 `hook panic: ...` 當成失敗，結果是 `INIT_FAILED`；Run 開始前 OnStop 的 panic 是 `Stopped(false)`。Go 在這兩處沒有 recover，panic 讓進程以 2 結束、不送 `Exiting`。
- E03-D055 NATS：照 E03-D006 與 Go `connectNATS`：client 名 `mftiko-session`、重連不設上限、每次 dial 最多 1 秒、失敗後 1 秒起加倍到 30 秒，dial 與等待都截到 `deadline_ms` 的剩餘；`deadline_ms` 為 0 時重試到連上或 Stop。dial 錯誤不傳出（文字可能含憑證），log 只寫 `nats connect failed`。與 Go 不同處：dial 本身也與 context 競速，Stop 立即結束進行中的 dial（Go 等這次 dial 回來，最多 1 秒）。async-nats 只開 `nkeys`、`ring`。`Hello.release` 是 `mftik-rt` 的版本（Go 用主模組版本或 VCS revision）。
- E03-D056 接線縫：`wiring::Wiring` 是 plane 呼叫各路徑的地方，位置同 `sdk/plane.go`。本票未接線時：bindings 解得開就算套用（不訂閱，`Live` 照樣回 `Applied`），listen TD、Recon 成功不做事，下單、撤單、`resolve`、`info`、MD 讀取回 `NotWired`，`order`／`open_orders`／`position`／`balance`／`book` 找不到。`resolve` 的 OnInit／OnStart 窗口已在 Context 檢查。hook 分派（Go `mdHooks`、`tdHooks`、`bookHook`）已寫在 plane，等 E03-06、E03-07 呼叫。整合測試以 socketpair 當控制 fd、真的 nats-server（釘 v2.15.0，與 mftiko go.mod 相同），operator、account、user JWT 在測試裡用 nkeys 簽，走 `user_jwt` 加 seed 的正式認證；找不到 nats-server（`MFTIK_NATS_SERVER` 或 `PATH`）測試失敗，不跳過，CI 的 rust job 以 `go install` 裝同一版。
- E03-D057 `Hello.release`（Yi Te 於 #418 確認）：`run(strategy, release)` 由策略傳入自己的版本或 revision，平台以 `WorkerStatus.release` 顯示，對應 Go 回報主模組版本或 VCS revision。`None` 或空字串時用 mftik-rt 的 `RELEASE`。

## 決定（2026-10-25，E03-D061–E03-D069）

E03-06 把 mdpath、bookpath 接到 NATS 與 dispatcher（`crates/mftik-rt/src/md/`）。語意對齊 mftiko `sdk/plane.go`（`applyBindings`、`handleCommand`、`Live`、`bookSnapshot`、`bookHook`）、`sdk/sessctx.go` 的讀取、`sdk/internal/mdpath`、`sdk/internal/bookpath`（PROTOCOL §7.1、§9.2.9、§9.3.2、§9.3.5、§9.4.2–§9.4.4、§9.5）。

- E03-D061 binding：Init 建好 dispatcher 後開 MD 路徑（Go `mdpath.Open`、`bookpath.Open`）。Start、Live、`Command.bindings` 都走 `Md::apply`：一次一個（Go `op` mutex），照 E03-D011 兩步：`Staged` → 逐一訂閱 → 有新訂閱就 flush → `apply_bound` → 退訂離開的；任何一步失敗 `apply_aborted`，退訂這次開的，集合與版本不變。Live 成功（含版本沒比較新）C4 回 `Applied`，失敗 C4 只記 `live not applied: ...`；`Command.bindings` 失敗記 `command bindings: ...`，session 不結束（§9.2.9、§9.3.5）。Init 之前（MD 路徑還沒開）套用回 Go 的 `bindings`，讀取回 `Stopping`，`book` 找不到，同 Go `sessCtx` 對 nil path 的處理；E03-05 的單元測試跟著改。訂閱失敗的文字是 `mdpath: subscribe {subject}: {async-nats 的錯誤}`，Go 直接回 nats.go 的錯誤，文字不同。`bindings_applied` 只在綁上比較新的版本時變，放在 `Wiring::watch_bindings_applied()`（tokio watch）給 E03-09 送 Report（§9.2.4）；本票不送 Report。
- E03-D062 flush：async-nats 的 `flush` 只把緩衝寫進 socket，不像 Go `FlushTimeout` 做 PING/PONG，回來時 server 不一定已經處理 SUB，平台在 gate 之後馬上發的 `md.f` 可能收不到。async-nats 沒有公開的 PING，所以 flush 之後再對 `md.v1.fetch._flush` 發一個 request：server 依序處理同一條連線，回 no responders（或任何回覆）時，之前的 SUB 都已生效。session 一定有 `md.v1.fetch.>` 的 publish 權限（E08-D018），venue 是大寫字母開頭的英數字（§2），不會是 `_flush`。整段上限 5 秒（Go `flushTimeout`），逾時或連線錯誤算 flush 失敗。
- E03-D063 `md.f`：每個訂閱一個 IO 執行緒上的 task，收到就問 `route`，`Md` 走 `push_md`、`Book` 走 `push_book`，不在 IO 執行緒解 payload；壞 envelope 也走 `push_md`，由 dispatcher 計入 `dropped` 並記 log（§9.4.3、E08-D049）。每次 push 進 dispatcher 之前先 `set_now_ns`（E03-D052），讀取回覆與 book 計時器排上 TD buffer 前也一樣。push 的錯誤（session 結束中）不理，同 Go。dispatcher 的 feed callback 在 plane 用 `Hooks::decode` 解碼後交給 `deliver_md`。
- E03-D064 book：bookpath 只在 dispatcher 執行緒上動：book callback、`md.view` 快照回覆（排上 TD buffer，同 Go）、計時器觸發（`TimerKey::Book`，到期時排上 TD buffer，同 Go 用 dispatcher `Timer`）。`RequestSnapshot` 以 mdpath 的 `book_snapshot` 發 `md.view`，等 `recv_window`；請求發不出去（例如沒有這個 feed）當場以 `Failed` 交回 bookpath，記 log 並退避，同 Go `fetch`。`Hook` 在 bookpath 的鎖放開後才交給 OnBook，所以 OnBook 裡 `book(uid)` 已含這則 delta。`Context::book` 回 `Option<&OrderBook>`（E03-D053）：第一次呼叫時取得 books 的 owned guard（tokio `Mutex`，`blocking_lock_owned`），存在這個 hook 的 Context 裡到 hook 結束，借出去的 book 因此不會被改；只有 dispatcher 執行緒取這把鎖，而且都在 hook 之間，所以不會卡住。`Context` 多一個私有欄位，公開簽名不變。
- E03-D065 商品列：MD 路徑不問 SYM（E08-D023、E08-D059）。`Wiring::row_stored(&Info)` 是給 E03-08 的接點：symclient 每次 `Effect::Stored { uid }` 之後，以 `Client::info(uid)` 的那一列呼叫它，任何執行緒、Init 前後都可以，不阻塞。它記下這個 UID 的 ticker（`UniversalTicker` 的正規寫法）、venue，以及待交給 bookpath 的 `price_tick`（同一個 UID 只留最新的）；bookpath 每次被呼叫前先 `resolve` 這些 tick（§9.5.7）。Tape、BookSnapshot 用 ticker，FetchKlines、FetchOpenInterest 用 venue；沒有這一列回 `UnknownUid`，同 Go 的 `symclient.ErrUnknownUID` 經 `asSDK`。Go 每次讀取時查 `sym.Info(uid)`，這裡讀 Stored 時留下的副本，每次 Stored 都會更新，所以值相同。mdpath 的錯誤照 Go `asSDK` 對應：`NoFeed`、`UnknownUid`、`Stopping`，其他保留文字（`Other`）。讀取的 callback 在 TD buffer 上跑；TD buffer 滿或已關時記 `md reply dropped: ...`，同 Go。
- E03-D066 測試：`src/md/tests.rs` 對真的 nats-server（不設認證）與同一進程裡的假 MD 對端，直接驅動 `Md` 與在自己執行緒上 `drive` 的 dispatcher：binding 先訂後換集合、退訂、訂閱失敗與 selector 不改版本；prehook 丟太舊的、壞 envelope 計入 `dropped`、壞 payload 不算；小 ring 丟最舊；book 由 `md.view` 同步、`not_synced` 退避 1 秒、斷號與 `reset` 重同步、沒有商品列時不要快照、`row_stored` 之後才要；讀取逾時、no responder、回覆錯誤、同步拒絕，回覆都在 dispatcher 執行緒。`tests/md.rs` 經 `mftik_rt::run` 端到端：Start 的 binding 在 gate 前訂好、`md.f` 經 prehook 到 OnTrade、Live 換集合回 `Applied`、selector 不回。nats-server 與假 shim 的 helper 從 `tests/session.rs` 移到 `tests/common/`。
- E03-D067 本地簿不加鎖（Yi Te 於 #420 確認）：bookpath 只在 dispatcher 執行緒上動（E03-D064），與 Go 的 `bookpath` 一樣不需要鎖（E08-D003）。它放在 `DispatcherCell` 裡：第一個碰到它的執行緒就是擁有者，其他執行緒碰到就 panic；裡面是 `RefCell`，hook 拿著簿時又去改簿也是當場 panic，不會變成死結再被 watchdog 判成 HUNG。`Context::book` 回 `BookView`（Go 是 `BookView`），deref 成 `OrderBook`，借自這個 hook 的 Context，不能帶出 hook；簿只在 hook 之間改，所以拿著的時候不會變。
- E03-D068 訂閱緩衝（Yi Te 於 #420 確認）：async-nats 每個訂閱預設最多緩衝 65536 則，超過就以 slow consumer 丟掉；Go 的 nats.go 沒有則數上限。連線設 `subscription_capacity` 為 2^20（`nats::SUBSCRIPTION_CAPACITY`）。訂閱的 task 收到就複製進 dispatcher，只有 IO 執行緒被餓住時才會積，該丟的地方是 32 MiB 的 MD ring 與 TD buffer（§9.5.2），不是 NATS client。tokio 的有界 channel 依需要配置，不會一開始就佔滿。
- E03-D069 SYM 接到 MD（合併 #419 與 #420 時）：`Wiring::open_sym` 在打開 SYM 路徑之前設 `Sym::on_stored`，把 symclient 每次 `Stored` 的那一列交給 MD 路徑的 `Rows`（E03-D065），bookpath 因此拿到 `price_tick`，讀取拿到 ticker 與 venue。沒有這一步，本地簿不會同步。

## 決定（2026-10-25，E03-D081–E03-D085）

E03-08 SYM 接線。語意對齊 mftiko PROTOCOL §7.3、§9.4 第 8 條（E08-D004、E08-D022–E08-D026）與 `sdk/internal/symclient/nats.go`、`sdk/plane.go`、`sdk/sessctx.go`。協定邏輯仍在 `mftik-core::symclient`（E03-D041–E03-D045），這裡只做 IO。

- E03-D081 等待時間（Yi Te 於 #411 確認）：`mftik_rt::sym::Sym` 執行 symclient 的 effect。帶 `resolve: Some(id)` 的 `Request`，等到該 resolve 所在 hook 的期限（Init／Start 的 `deadline_ms`），沒有期限才用 effect 的 5 秒；hook 的 context 被取消（Stop）時立刻結束。背景重拿（`resolve: None`）用 5 秒。resolve 的執行緒只等 `Resolved{id}`，回覆、通知、計時器都在 IO 執行緒處理，所以結果可以來自別的請求的回覆。逾時、取消、沒有 responder 的文字照 Go：`context deadline exceeded`、`context canceled`、`nats: no responders available for request`（與 E03-D013 相同），symclient 再加上 `symclient: request sym: ` 前綴。
- E03-D082 訂閱與計時器：`Subscribe` 先 subscribe 再 flush（最多 5 秒，Go 的 `FlushTimeout`），flush 回來才 `on_subscribed`，之後的訊息依到達順序交給 `on_changed`。約 5 分鐘的比對是 timer 表的 `TimerKey::Sym`。
- E03-D083 `Stored` 的通知：`Sym::on_stored(Arc<dyn Fn(u32, &Info)>)` 是給 MD 路徑（E03-06）的縫：每個 `Stored{uid}` 呼叫一次，帶當時存的列，E03-06 在裡面做 `bookpath.resolve(uid, info.price_tick())`（E03-D045）。session 中它在 IO 執行緒、依存入順序呼叫，不可阻塞、不可呼叫 resolve。再設一次會取代前一個。
- E03-D084 重連：`nats::dial` 多收一個 `Reconnected` callback，掛在 async-nats 的 `event_callback`。async-nats 第一次連上也報 `Connected`，所以每條連線第一個 `Connected` 跳過，之後每個都呼叫 `Sym::on_reconnect`，對應 Go 的 `ReconnectHandler` 呼叫 `Client.Reconnect`。`Sym` 在 Init 連上 NATS 之後才 `open`，之前的重連不做事。
- E03-D085 錯誤對應（Go `asSDK`）：`NotInInit`、`UnknownUid` 對應 `Error` 同名；`Closed`（還沒有連線，或 client 已關）是 `Stopping`，對應 Go `p.sym == nil` 的 `ErrStopping`；其他（`NotFound`、`InvalidTicker`、`Failed`）是 `Error::Other`，文字不變，所以策略回傳查不到的錯誤時 `INIT_FAILED` 的 reason 是 `symbol: no such instrument: not_found`。`info` 在 Init 之前回 `UnknownUid`（Go 是 `ErrStopping`；Init 之前沒有 hook，策略拿不到 Context，不影響行為）。
