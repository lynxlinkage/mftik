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
- E01-D003 套件名沿用 `mftik`（import `mftik`），版本從 0.12.0 起，E04 才發佈。（「版本從 0.12.0 起」已由 E04-D008 取代：版本 0.14.0。）
- E01-D004 整包複製 mftiko `contract/proto/mftiko`，釘在 BullionBear/mftiko `e7b3543e2269c4ae8c10631edd448bdb86c52e2c`，同步腳本記錄來源 commit。
- E01-D005 `build.rs` + prost-build，protoc 用 `protoc-bin-vendored`，產生的程式碼不進 git。
- E01-D006 `rust-toolchain.toml` 釘目前 stable、edition 2024；Python 用 uv + maturin，lint 用 ruff；CI 在 GitHub Actions ubuntu，clippy 警告即失敗。
- E01-D007 CI 先備好以 `go install` 從釘住的 mftiko commit 編 `mftiko-conformance` 的步驟，E05 才接上執行。（「E05 才接上執行」已由 E03-D007 取代：E03-10 起 CI 以 Rust 版標準策略執行，見 E03-D103。）
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

## 決定（2026-10-25，E03-D071–E03-D076）

E03-07 TD 接線。語意對齊 mftiko PROTOCOL §7.2、§9.2.5、§9.4.5–§9.4.7、§9.6（E08-D001、E08-D014、E08-D041–E08-D045）與 `sdk/plane.go`、`sdk/sessctx.go`、`sdk/internal/tdpath`。OMS、ledger、水位、退避在 `mftik-core` 的 `tdpath`；`mftik-rt/src/td` 只執行它的 effect（E03-D001）。

- E03-D071 餵入與執行緒：Init 以 `InitPayload.accounts` 開 `tdpath`（Go `tdpath.Open`，錯誤文字結束 Init）。`td.evt` 每個帳號一個 reader task，在 IO 執行緒解碼，`api_id` 取自該訂閱的 subject（§9.6.2），解不開記 `td.evt: ...`。`td.evt`、`td.order` 的結果、`td.view` 的結果、退避觸發，各以一個 TD buffer 項目推上 dispatcher，在 dispatcher 執行緒餵給 `tdpath`；一個項目最多跑一個策略 hook，hook 之後 `end_hook`，釋放的下一步再排一個項目，所以每個 hook 有自己的預算，hook 之間 TD 照樣優先於 MD。釋放鏈還沒結束時到的輸入照到達順序等著，鏈結束才餵（`tdpath` 的「hook 返回、`end_hook` 之前不餵」）。與 Go 不同處：Go 在 NATS goroutine 上就分出扣住或放行，這裡在 dispatcher 取出時才分。OnStart 期間 dispatcher 還沒跑，事件留在 TD buffer 而不是扣住區；上限同是 65536、超過同樣以 `FAILED`「td buffer full」結束。OnStart 期間 recon 失敗，退避從 OnStart 返回、結果被餵入時才起算（Go 立即起算）。訂閱的 client 端緩衝是 async-nats 預設的 65536 則，reader 持續搬進 TD buffer；Go 設成不限。
- E03-D072 收尾：`unsubscribe` 是 dispatcher 的 `unsubscribe_td`，只在 `DONE`、`FAILED` 的交付之前呼叫（§9.2.5、E08-D003）：停掉 reader 並退訂、解除退避計時器，之後到的事件與回覆不再推上；TD buffer 裡已經收到的照常餵入並交付。`td.view` 回覆在這之後被餵入時照樣交給 OnRecon，但釋放在下一則扣住事件停下，與 Go 的 `releaseStep` 看到 path 已關相同。下單、撤單、`request_recon` 之後回 `Stopping`，不再送出任何請求。可重複呼叫。E03-09 的結束流程經 `Dispatcher::on_stop` 呼叫它。扣住區滿而 `FAILED` 時 `tdpath` 已經停下，TD buffer 裡剩下的事件餵入也不交付；Go 已經排進 dispatcher 的 live 事件仍會交付。要對齊得改 E02 的 `tdpath`，本票不動。
- E03-D073 `td.order` 與 `td.view`：`tdpath` 回的 `SendOrder` 依序交給一個 task：先訂一個 reply inbox、再 publish，兩者走同一條命令佇列，下一筆在這筆 publish 之後才送，回覆可以重疊（Go `requestInOrder`）。等待從 effect 產生時起算 `recv_window`，包含排隊（Go 的 round context）。503 是 no responder（101 本地拒絕）；逾時、送不出、其他 status、body 不是 `OrderReply` 都是沒有回答：Submit 變 `ORDER_STATUS_UNKNOWN`，撤單不變、不通知（E08-D043、E08-D045）。`td.view` 用 async-nats 的 request，等 `recv_window`，任何失敗都是 `ReconOutcome::Failed`，由 `tdpath` 退避。recon 回覆推上 TD buffer 之後記 `td.view: queued`（Go `ask`，conformance 的 buffer_priority 等這行）。`client_order_id` 用 `cloidgen::next`，`now_ms` 取注入時鐘（§9.4.6）。
- E03-D074 訂閱的 flush（需要 Yi Te 確認）：async-nats 的 `flush` 只把 buffer 寫進 socket，不做 PING/PONG；Go 的 `FlushTimeout` 是一次 round trip。session 的帳號不能 publish 到自己的 inbox（`sts.SessionPermissions`），也就做不出回音式的屏障。`td.evt` 的 SUB 與之後的 `td.view` 請求走同一條連線，server 依序處理，所以水位不受影響：在 SUB 生效之前由別的連線發出的事件，在 TD 收到 ReconRequest 之前，會含在快照裡。差在 `listen` 返回（gate 已印）之後立刻由別的連線 publish 的事件，理論上可能早於 server 處理 SUB 而漏掉；Go 不會。
- E03-D075 錯誤與還沒開的 path：錯誤照 Go `asSDK`：`UnknownAccount`、`AlgoNotDeclared`、`Stopping` 對到 `sdk.Err*`，其餘保留 `tdpath` 的文字，cloidgen 的錯誤保留 cloidgen 的文字（Go 原樣回傳）。Init 之前下單、撤單回 `Stopping`、讀取找不到，同 Go 的 nil path；`NotWired("td")` 不再出現，E03-05 的 plane 單元測試改成 `Stopping`。沒有 path 時 `listen_td`、`recon` 不做事（C4 不會在 Init 成功前呼叫 Start，只有 plane 的單元測試走到）。
- E03-D076 flush 一律是往返（Yi Te 於 #421 確認，合併 #420 與 #421 時）：E03-D062 的往返搬到 `nats::barrier`，MD 的 binding、TD 在 gate 之前的 `td.evt` 訂閱、SYM 的 `sym.v1.changed.*` 訂閱都用它，不再只寫 socket。三者共用同一條連線，所以 gate 之前的訂閱都已在 server 上。subject 是 `nats::BARRIER_SUBJECT`（`md.v1.fetch._flush`），前提同 E03-D062。

## 決定（2026-10-25，E03-D081–E03-D085）

E03-08 SYM 接線。語意對齊 mftiko PROTOCOL §7.3、§9.4 第 8 條（E08-D004、E08-D022–E08-D026）與 `sdk/internal/symclient/nats.go`、`sdk/plane.go`、`sdk/sessctx.go`。協定邏輯仍在 `mftik-core::symclient`（E03-D041–E03-D045），這裡只做 IO。

- E03-D081 等待時間（Yi Te 於 #411 確認）：`mftik_rt::sym::Sym` 執行 symclient 的 effect。帶 `resolve: Some(id)` 的 `Request`，等到該 resolve 所在 hook 的期限（Init／Start 的 `deadline_ms`），沒有期限才用 effect 的 5 秒；hook 的 context 被取消（Stop）時立刻結束。背景重拿（`resolve: None`）用 5 秒。resolve 的執行緒只等 `Resolved{id}`，回覆、通知、計時器都在 IO 執行緒處理，所以結果可以來自別的請求的回覆。逾時、取消、沒有 responder 的文字照 Go：`context deadline exceeded`、`context canceled`、`nats: no responders available for request`（與 E03-D013 相同），symclient 再加上 `symclient: request sym: ` 前綴。
- E03-D082 訂閱與計時器：`Subscribe` 先 subscribe 再 flush（最多 5 秒，Go 的 `FlushTimeout`），flush 回來才 `on_subscribed`，之後的訊息依到達順序交給 `on_changed`。約 5 分鐘的比對是 timer 表的 `TimerKey::Sym`。
- E03-D083 `Stored` 的通知：`Sym::on_stored(Arc<dyn Fn(u32, &Info)>)` 是給 MD 路徑（E03-06）的縫：每個 `Stored{uid}` 呼叫一次，帶當時存的列，E03-06 在裡面做 `bookpath.resolve(uid, info.price_tick())`（E03-D045）。session 中它在 IO 執行緒、依存入順序呼叫，不可阻塞、不可呼叫 resolve。再設一次會取代前一個。
- E03-D084 重連：`nats::dial` 多收一個 `Reconnected` callback，掛在 async-nats 的 `event_callback`。async-nats 第一次連上也報 `Connected`，所以每條連線第一個 `Connected` 跳過，之後每個都呼叫 `Sym::on_reconnect`，對應 Go 的 `ReconnectHandler` 呼叫 `Client.Reconnect`。`Sym` 在 Init 連上 NATS 之後才 `open`，之前的重連不做事。
- E03-D085 錯誤對應（Go `asSDK`）：`NotInInit`、`UnknownUid` 對應 `Error` 同名；`Closed`（還沒有連線，或 client 已關）是 `Stopping`，對應 Go `p.sym == nil` 的 `ErrStopping`；其他（`NotFound`、`InvalidTicker`、`Failed`）是 `Error::Other`，文字不變，所以策略回傳查不到的錯誤時 `INIT_FAILED` 的 reason 是 `symbol: no such instrument: not_found`。`info` 在 Init 之前回 `UnknownUid`（Go 是 `ErrStopping`；Init 之前沒有 hook，策略拿不到 Context，不影響行為）。

## 決定（2026-10-25，E03-D091–E03-D096）

E03-09 Report 與結束整合。語意對齊 mftiko PROTOCOL §5.3、§9.2.4、§9.2.5、§9.5.5、§9.5.6（E08-D003、E08-D015）與 `sdk/run.go`、`sdk/plane.go`。

- E03-D091 Report 的時機（需要 Yi Te 確認）：`mftik_rt::report` 是一條 std 執行緒 `mftik-report`，同 Beat 不經 tokio、不經 dispatcher，只等 fd 3 的寫鎖（E03-D003）。C4 執行 `CallRun`（`Stage(running)` 之後）時啟動，第一則立刻送；之後每 1 秒（`REPORT_POLL`）讀一次 dispatcher 的 `stats` 與 `bindings_applied`，與上一則送出的不同就送，沒變則在上一則之後 5 秒（`REPORT_EVERY`）到期時送，所以兩則之間不超過 5 秒。綁上比較新的版本時，`Wiring::watch_bindings_applied` 在 IO 執行緒喚醒它，不等下一次輪詢。Init、Start 期間不送。`hook_blocked_ms` 在 hook 跑著時每次讀都不同，所以那段時間每秒一則。Go 的週期與起點我沒有對照原始碼，以 §9.2.4 的「有變化就送，至少每 5 秒一次」為準。
- E03-D092 內容：`queues` 依序是 `td`、`md`，各帶 dispatcher `Stats` 的 `depth`、`dropped`、`max_age_ms`；另有 `hook_slow`、`hook_blocked_ms`、`bindings_applied`（還沒綁上任何快照是 `None`）。`Stats` 每次讀都清掉 `max_age_ms`，兩則 Report 之間沒送出的讀取取最大值，交給下一則，所以 `max_age_ms` 是「上一則 Report 以來交給 hook 的最老事件」（§6.1）。`td.dropped` 一律是 0：TD buffer 滿不丟，直接以 `FAILED` 結束（E08-D015）。
- E03-D093 Exiting 之後沒有 Report：driver 寫 `Exiting` 之前呼叫 `Reporter::close`。Report 執行緒在送出期間握著同一把鎖，所以正在寫的那則先寫完，之後不再開始；`exit_with` 的 seal 照樣擋住其他寫入（E03-D032）。寫入失敗（含 `Sealed`）時執行緒結束，不重試，同 Beat。
- E03-D094 `Report.hook`（需要 Yi Te 確認）：留空字串。dispatcher 只知道有沒有 hook 在跑、跑了多久，不知道是哪一個；要填得在 `dispatch` 的 TD 項目與 MD 回呼帶名字，E02 的 API 要改，本票不動。
- E03-D095 結束順序：結束路徑沒有新的狀態，順序由 C4（E03-D021–E03-D027）與 dispatcher（E02-03）決定，本票確認並補端到端測試。Stop、fd 3 EOF、SIGTERM 在 Run 中是 `Cancel` → `Dispatcher::exit`；`exit()`、`fail()`、`Command.fail`、hook panic、TD buffer 滿都是 dispatcher 的結束原因。dispatcher 停收 MD、丟掉 ring、等目前的 hook 返回，`wait_run` 才回，C4 才送 `Exiting`；`DONE`、`FAILED` 在 `on_stop` 裡先 `unsubscribe_td`、交付 TD buffer，再 OnStop；`CRASHED` 不交付但跑 OnStop；之後 `Stopped`、exit。`HUNG` 由 watchdog 發出，C4 送 `Exiting(HUNG, "hook timeout")`，照 E03-D021 的對應接著送 `Stopped(false)`，再 seal、`_exit(21)`，不跑 OnStop、不等那個 hook（E03-D005）。票上 HUNG 的順序沒寫 `Stopped(false)`；它表示 OnStop 沒跑完，與 `INIT_FAILED` 不跑 OnStop 時相同。
- E03-D096 測試：`tests/ending.rs` 經 `mftik_rt::run` 端到端，對每條路徑檢查 frame 順序與 exit code：Stop、fd 3 EOF（等跑著的 hook 返回才 `Exiting`，之後交付它排的 TD、OnStop、`Stopped(true)`、0）；`exit()`、`fail()`（TD 交付後 OnStop，0、30）；`Command.fail`（Relay 回聲之後 `FAILED`，30）；hook panic（`CRASHED`、OnStop、不交付 TD，20）；OnStart 錯誤（`INIT_FAILED`、OnStop，10，沒有 Report）；`HUNG`（`Stopped(false)`、不跑 OnStop、不交付 TD，21）；TD buffer 滿（`FAILED`「td buffer full」，交付的正好是 `TD_CAP` 則，OnStop，30）；EOF 之後 OnStop 超過 hook 預算（沒有 `Stopped`，21，E03-D027）。「等 hook 返回」的證據：測試等到 Report 的 `hook_blocked_ms > 0` 才送 Stop，hook 返回前寫 volume，收到 `Exiting` 時讀 volume 必須已寫好。Report 的測試檢查兩個 queue、`bindings_applied` 隨 Live 更新、不比較新的 Live 不改、150 ms 的 hook 算一次 `hook_slow`、沒有變化時兩則相同的 Report 相隔約 5 秒。共用的假 shim 把 Report 收起來，讀到 `Exiting` 之後還有 Report 就讓測試失敗。

## 決定（2026-10-25，E03-D101–E03-D103）

E03-10 Rust 版 conformance 標準策略與 CI（E03-D007）。語意對齊 mftiko PROTOCOL §9.8（E08-D028–E08-D039、E08-D064）與 `examples/strategies/conform/main.go`、`internal/sts/conformance`，對照的是 `contract/proto/SOURCE` 釘住的 commit。

- E03-D101 標準策略：`examples/conform` 是 workspace 的 `mftik-conform` crate，binary 名 `conform`，經 `mftik_rt::run` 掛上，`Hello.release` 用 mftik-rt 的 `RELEASE`。行為與 Go 版一對一：`InitPayload.params` 以 serde_json 解成同名欄位，沒給的是零值，不認得的欄位略過（同 Go `json.Unmarshal`）；`until_seq` 解得開但策略不讀，Go 也不讀。OnStart 的順序同 Go：`beat` 先印再睡，其他先睡 `start_sleep_ms`；`priority` 先 `request_recon`；`priority`、`prehook`、`ring`、`overflow`、`ctx` 呼叫 `resolve("Binance_Spot_BTCUSDT")` 讓工具扣住 OnStart；送 `order_count` 筆限價買單；`probe_reads` 打 `tape` 與 `fetch_klines`，回呼裡印 `view`、`fetch`；最後 `self_exit` 時 `exit()`。OnOrder、OnPosition 印 `ctx`（呼叫當下本地狀態是否已含這則），`ctx` 情境為 false 時 `fail("ctx")`。stdout 每行一個 JSON，欄位與 Go 的 `observation` 相同、順序相同（`hook`、`ctx`、`seq`、`incarnation`、`view`、`fetch`），零值與空的省略，bool 只要設了就寫（含 false）。§9.8 列的 `md`、`queued` 不是 Go 策略寫的：`queued` 是 fakeworker `-violate td_overflow` 的輸出，`gate` 由 SDK 印。
- E03-D102 hook 開始與結束不喚醒（E02 `dispatch` 的實作修正，語意不變）：`begin_hook`、`begin_hook_at`、`finish_hook` 不再 `notify_all`。只有 watchdog 讀 `hook_on`，而它要等注入時鐘越過預算才會觸發，那由 `set_now_ns` 喚醒；先前每個 hook 叫醒 watchdog 兩次，每則 TD 兩次 context switch。`td_overflow` 在 OnStart 被扣住時灌進 65537 則 `td.evt`，`FAILED` 之後在 OnStop 的 hook 預算（`hook_timeout_s` 1 秒）裡要交付 TD buffer 的 65536 則：release 由 792 ms 降到 25 ms，debug 由超過 1 秒（exit 21）降到 131 ms。只測 dispatcher 的 65536 則空 hook，248 ms 降到 5 ms。
- E03-D103 CI：`mftiko-conformance` job 拿掉 `if: false`，Go 1.26 以 `scripts/install-mftiko-conformance` 從釘住的 commit 安裝，`cargo build --release -p mftik-conform` 之後跑 `mftiko-conformance -json report.json -- target/release/conform`；任一項 fail 時工具 exit 1，job 失敗；`report.json` 不論成敗都上傳成 artifact。用 release 是為了預算有餘裕，debug 也全過。本機對釘住的工具：`flow`、`beat`、`deadline`、`shutdown`、`buffer_priority`、`prehook`、`td_overflow`、`cloid`、`subject`、`recon`、`ctx` 全部 pass，沒有 skip。

## 決定（2026-10-26，E04-D001–E04-D014）

Yi Te 於 #386 確認 E04-D001–E04-D010，於 #426 確認 E04-D011–E04-D014。E04 的共同決定與 E04-01 的決定。與 Go 版語意不同之處先回報再做。

- E04-D001 佈局與分工：maturin mixed project，`python-source = "python"`，純 Python 套件 `python/mftik/`（`__init__.py`、`run.py`、`py.typed`、原生模組的 `.pyi`），原生模組 `mftik._mftik`。`mftik-py` 依賴 `mftik-rt`，只做綁定；協定邏輯與 IO 不進 `mftik-py`，`mftik-rt` 只為綁定加必要的縫。`python/smoke_import.py` 移到 `tests/python/`。panic 維持 unwind（E04-D004 靠它）。
- E04-D002 執行緒、GIL 與結束：`mftik.run` 在呼叫端（Python 是主執行緒）放開 GIL 進 `mftik_rt::run`（E03-D003）。每個 hook（含讀取的 callback、`after`、`post`）取 GIL，返回就放；hook 與 hook 之間不持 GIL。`resolve` 等待期間放 GIL（`resolve` 的綁定在 E04-04）。SDK 不另開 Python 執行緒。進入點把 `sys.stdout`、`sys.stderr` 設成 line buffering。E03-D005 的 `_exit` 不跑 Python 的 finalize，Python 的 stdout 接 pipe 時是 block buffered，緩衝裡的行會掉；所以 `mftik-rt` 的 `exit_with` 在 `_exit` 之前呼叫綁定註冊的 callback，沒有 hook 在跑時取 GIL flush `sys.stdout`、`sys.stderr`。HUNG 或 hook 仍在跑時跳過，那時主執行緒握著 GIL，取 GIL 會卡住。
- E04-D003 Strategy 與 hook：`mftik.Strategy` 是純 Python 基底類別，`on_init`、`on_start`、`on_stop` 必須實作。17 個選用 hook 是 Go 名稱的 snake_case：`on_best_quote`、`on_trade`、`on_agg_trade`、`on_kline`、`on_ticker`、`on_liquidation`、`on_funding_rate`、`on_open_interest`、`on_greeks`、`on_mark_price`、`on_book`、`on_order`、`on_fill`、`on_position`、`on_balance`、`on_recon`、`on_reject`，簽名 `(self, ctx, ev)`。只註冊子類別覆寫的，在 OnInit 之前判斷一次，對應 `Strategy::handlers`；沒有 hook 的 topic 在解碼前就跳過（E08-D005）。各 hook 的事件轉換與呼叫在 E04-03；本票註冊之後，選用 hook 只取 GIL 就返回。
- E04-D004 例外：OnInit、OnStart 拋出的例外是 `INIT_FAILED`，`Done.error` 是 `{型別名}: {str(e)}`，traceback 只寫 stderr。其他 hook（含讀取的 callback、`after`、`post`）拋出的例外交給 dispatcher 當 panic，結果是 `CRASHED`，reason `hook panic: {型別名}: {str(e)}`，OnStop 照跑、TD 不交付（E03-D095）。OnStop 拋出比照 Rust 的 OnStop panic（`dispatch: onstop panic: ...`，`Stopped(false)`，exit code 不變）。`SystemExit`、`KeyboardInterrupt` 也一樣處理，`sys.exit` 不等於 `ctx.exit()`。例外文字會進 `Done.error`、`Exiting.reason`，不得帶秘密，同 Go。traceback 用 `traceback.print_exception`，不用 `PyErr_Print`（後者把 `SystemExit` 當成程序結束）。
- E04-D005 Context 生命週期：Python 的 `Context` 只在該 hook 期間有效。hook 返回之後再用，或在 dispatcher 以外的執行緒用，丟 `mftik.ContextError`，不 panic、不 UB。跨執行緒唯一入口是 `ctx.poster()` 回的 `Poster`，`Poster.post(fn)` 可從任何 Python 執行緒呼叫（Go `Post`）。`ctx.book(uid)` 回的 `BookView` 同樣只在該 hook 期間有效（E03-D067）；`book` 的綁定在 E04-05。
- E04-D006 訊息型別（原第 7 題；Yi Te 2026-10-26 改用 prost，取代原本的官方 protobuf）：proto 訊息（`md.v1` 的 payload、TapePage、BookSnapshot、Klines，`td.v1` 的 Order、Position、Balance、Fill、Recon 等）不用官方 protobuf，由 `mftik-proto` 的 prost 型別產生 frozen pyclass，欄位名稱與 proto 相同。轉換是把 prost 的值搬進 pyclass，不經過序列化；wheel 不依賴 `protobuf`。產生方式、enum／oneof／optional 的對應與 `.pyi` 在 E04-02 定，proto 升版時重新產生、不手寫；`mftik-proto` 的預設 feature 不依賴 pyo3。SDK 自己的型別（`Md`、`Td`、`Feed`、`BookUpdate`、各 Event、`Info`、`Order`、`TapeQuery`、`BookView`）同樣用 frozen pyclass。本票不手寫這些 pyclass。
- E04-D007 驗收：Python 版 conformance 標準策略與它的 CI 照 E03-D007 留在 E05。E04 的驗收是 E04-01 的 Python e2e 測試架與各票的測試，每個 PR 的 CI 都跑；Rust 版 conformance 照跑。
- E04-D008 發佈（原第 5 題）：版本 0.14.0。PyPI 已有舊 SDK 的 0.12.0、0.12.1，git 有 v0.13.0-a1–a4；1.0.0 留到 E05 實機跑過。取代 E01-D003 的「版本從 0.12.0 起」。wheel 出 manylinux_2_28 x86_64（必須）與 macOS arm64（本機開發、型別檢查與 `python -m mftik.pack` 用，不保證能跑 session），不出 sdist。PyPI 用 trusted publishing，PyPI 專案 owner 要設定一次。發佈工作是 E04-06；在那之前這棵樹的套件版本維持 0.12.0，不在 E04-01 改 workspace 版本（那會改到 `mftik-rt` 的 `RELEASE`）。（「在那之前這棵樹的套件版本維持 0.12.0」由 E04-06 結束，版本改為 0.14.0。）
- E04-D009 OCI 打包（原第 6 題）：mftik 自己做（`python -m mftik.pack`），不改 mftiko-pack；使用者只裝 mftik 就能打包，不用裝 Go。manifest 與推送邏輯因此有兩份，CI 以釘住的 mftiko 的 `internal/platform/artifact` 拉取、檢查、解開來對照（PROTOCOL §8.3）。實作是 E04-07。
- E04-D010 並行與決策區段：E04-01、E04-02、E04-06、E04-07 同時開工；E04-03、E04-04、E04-05 等 E04-01、E04-02；E04-06 的發佈等 E04-01–E04-05 合併；E04-07 的 CI 端到端檢查等 E04-01。各票的決策區段：E04-01 從 D011 起，E04-02 從 D021 起，依此類推，E04-07 從 D071 起。第一張合併的 PR 把 E04-D001–E04-D010 寫進 `docs/DECISIONS.md`；後合併的 PR 以 merge main 解 DECISIONS 衝突，不 rebase。
- E04-D011 `after(delay, fn)` 的 `delay` 是秒（float），與 `time.sleep` 一致；內部轉成 `Duration`（再由 runtime 轉 ns）。負數或 NaN 丟 `ValueError`，訊息是 `delay must be a finite number of seconds >= 0`。±inf 與轉不進 `Duration` 的有限值（`Duration::try_from_secs_f64` 拒絕的）用同一個 `ValueError`：它們不是有限的非負秒數，沒有第二種可表示的結果。0 可以。
- E04-D012 import、建構或型別檢查失敗的 `Done.error` 為 `load module:Class: {型別名}: {str(e)}`，traceback 只寫 stderr。仍進 `mftik_rt::run`，OnInit 回這個錯，session 以 `INIT_FAILED` 結束。不在 Hello 之前 exit 1。
- E04-D013 抽出的測試架 crate 名為 `mftik-testkit`，`publish = false`，只當 dev-dependency。假 shim 可以起任意 argv。E03-D066 放在 `crates/mftik-rt/tests/common/` 的 nats-server 與假 shim 改由它提供；`mftik-rt` 用到那份 helper 的測試改依賴它。
- E04-D014 CI 的 python job 只跑 Python 3.11（最低支援版本，abi3）；版本矩陣留到 E04-06。

## 決定（2026-10-26，E04-D021–E04-D024）

E04-02 Python 端的型別與錯誤。只做型別與轉換，不接 hook。

- E04-D021 產生方式：`mftik-py` 的 build.rs 讀 descriptor 產生 frozen pyclass wrapper、`From` 轉換與 `.pyi`；`mftik-proto` 不碰 pyo3。產生碼不進 git（E01-D005）。
- E04-D022 proto enum 對到 `enum.IntEnum`；收到未知值不丟例外，保留原始 int（同 Go 保留未知 enum 數值）。
- E04-D023 欄位對應：`optional` 與沒設的 message 欄位為 `None`；oneof 每個分支各是一個屬性，未選為 `None`，另有屬性回傳選中分支名（未設為 `None`）；`repeated` 為 `tuple`；`map` 為 `dict`，每次讀取回新的副本。
- E04-D024 一期不提供以 prost 編解碼 bytes 的進出（Go `proto.Marshal` 對應），有需要再加。

## 決定（2026-10-26，E04-D031–E04-D034）

E04-03 選用 hook 的交付。Yi Te 於 #428 確認。

- E04-D031 是否覆寫只看類別：`type(s).on_x is not Strategy.on_x`，OnInit 前判斷一次；執行期掛在 instance 上的屬性不算（同 Go 註冊時固定）。
- E04-D032 `async def` 的 hook（含 `on_init`、`on_start`、`on_stop`）在註冊時以 `TypeError` 拒絕，session 為 `INIT_FAILED`；SDK 不開 event loop。
- E04-D033 hook 的回傳值忽略，不檢查（同 Go hook 無回傳值）。
- E04-D034 每次交付建立新的 frozen 事件物件，不重用；BestQuote、Trade、OrderEvent 每則從轉換到呼叫的耗時記在 PR，無硬門檻（同 E02-D004）。

## 決定（2026-10-26，E04-D041–E04-D044）

Yi Te 於 #429 確認 E04-D041–E04-D044。E04-04 Context 的商品、下單、本地狀態與 volume。

- E04-D041 `submit` 只收 `mftik.Order`（E04-02），不提供 kwargs 快捷；型別不對丟 `TypeError`。
- E04-D042 `open_orders` 回 `tuple[Order, ...]`，無掛單為空 tuple（同 E04-D023）；順序照 `mftik-rt`，不另排序。
- E04-D043 `resolve` 沿用 runtime 的逾時與 hook 預算，Python 不加 timeout 參數。
- E04-D044 volume：`dir`、`volume_dir`、`input_dir` 收 `str | os.PathLike`；`data` 收 bytes-like（`bytes`、`bytearray`、`memoryview`），`str` 丟 `TypeError`，不隱式編碼；`read` 回 `bytes`，找不到回 `None`。

## 決定（2026-10-26，E04-D051–E04-D054）

E04-05 Context 的 MD 讀取、callback 與 BookView。Yi Te 於 #430 確認。

- E04-D051 `BookView.bids(limit)`、`asks(limit)` 回 `list`（當下的副本），不回迭代器，hook 之後仍可用。
- E04-D052 每一檔是 `tuple`，欄位同 Go 的 level（價格 tick、數量），不另包 pyclass。
- E04-D053 `mftik.topic` 的常數是 `str`，不用 Enum；`kline(interval)` 回 `str`。
- E04-D054 讀取的 callback 呼叫時只檢查 callable，不是則同步丟 `TypeError`；參數個數不預先檢查，執行時出錯照 E04-D004（`CRASHED`）。

## 決定（2026-10-26，E04-D061–E04-D065）

Yi Te 於 #431 確認。E04-06 的 wheel 與 PyPI 發佈。

- E04-D061 PyPI 用 trusted publishing（GitHub OIDC），workflow `release.yml`、environment `pypi`；不放 API token。專案 owner 設定 publisher 的步驟寫在 PR。
- E04-D062 PyPI 上 0.12.x 以前的舊版保留、不 yank；0.14.0 的 README 註明為 Rust 重寫、與舊 API 不相容。
- E04-D063 每個 PR 的 CI 建 manylinux_2_28 x86_64 與 macOS arm64 兩個 wheel，各裝進乾淨 venv 跑 smoke。
- E04-D064 metadata：classifiers 為 Python 3.11–3.13、Development Status :: 3 - Alpha；`requires-python >=3.11` 與 abi3-py311 一致。
- E04-D065 第一次發佈：PR 合併後在 main 打 `v0.14.0`，等 Yi Te 確認才打。

本票把 `Cargo.toml` 的 workspace version、`pyproject.toml`、`tests/python/smoke_import.py` 的期望值、`mftik.__version__`（`python/mftik/_version.py` 的 fallback；裝進 wheel 之後由套件 metadata 提供）一起改成 0.14.0。CI 的 `scripts/release-check versions` 檢查四者一致。`release.yml` 只在 `v*` tag 上建 E04-D063 的兩個 wheel、跑 smoke，tag 不是 `v` 加這個版本、或那個 commit 不在 main，就失敗，然後以 trusted publishing 上傳。不出 sdist，workflow 裡沒有 API token，也不打 tag。E01-D002 刪掉的舊 `release.yml`（映像、Strategon、compose）不復原。E04-D008 的「在那之前套件版本維持 0.12.0」到此結束。E04-D014 留到本票的版本矩陣是 E04-D063 的兩個 wheel；python job 的完整測試仍只跑 Python 3.11。

## 決定（2026-10-26，E04-D071–E04-D076）

Yi Te 於 #432 確認。E04-07 的 OCI 打包。

- E04-D071 `mftik.pack` 用純 Python、只用標準函式庫實作，含最小 OCI 推送 client（blob、manifest 上傳；basic 與 token 交換）。不放進 `mftik._mftik`。
- E04-D072 依賴層以 `pip download --only-binary=:all: --platform manylinux_2_28_x86_64 --python-version <版本>` 取得 wheel 並解開到 `site-packages/`；只有 sdist 的依賴直接報錯，不在本機編譯。
- E04-D073 CPython 預設 3.12，允許 3.11–3.13；python-build-standalone 的版本與 sha256 寫死在程式，下載後驗證，不符即失敗。
- E04-D074 pyc 以 `unchecked-hash` 模式預先編譯。
- E04-D075 下載的 CPython 與 wheel 快取在 `~/.cache/mftik/pack`，以 sha256 為 key。
- E04-D076 CI 的 mftiko checkout 釘在 commit `dd73a2d421ca`，需要時手動更新。

本票的 `python -m mftik.pack` 依這些決定打包。三層是 runtime（釘住的 `install_only_stripped` 加上同版本的 mftik wheel）、依賴（`site-packages/`）、策略（`app/`）。python-build-standalone 裡含 `..` 的 symlink 以檔案複本放進層，解開時才過得了 §8.3。sdk、依賴、策略的 `.py` 以 unchecked-hash 預先編成 `.pyc`；manifest 設 `PYTHONDONTWRITEBYTECODE=1`，worker 不改 rootfs。`contracts` 來自 `mftik._mftik.session_contracts`，與 `Hello.contracts` 同源。CI 的 `pack` job 推到本機 `registry:2`，用釘住的 mftiko `internal/platform/artifact` 拉取、檢查、解開，再以假 shim 在空的 PATH 跑完 Init、Start、Stop。同一份輸入打兩次，digest 相同。不推送任何對外的 image 或 tag。

## 決定（2026-10-26，E05-D001–E05-D014）

Yi Te 於 #387 確認 E05-D001–E05-D010，於 #440 確認 E05-D011–E05-D014。E05 的共同決定與 E05-01 的決定。與 Go 版語意不同之處先回報再做。

- E05-D001 Rust 版 conformance 保留：`examples/conform` 的 job 照跑，守 `mftik-rt`；Python 版守 PyO3 那層。兩者都在每個 PR 的 CI 跑。
- E05-D002 Python 版 conformance 在 3.11、3.12、3.13 三版各跑一次，並行。0.14.0 宣告支援 3.11–3.13（E04-D064），abi3 wheel 要在三版上真的跑過。
- E05-D003 conformance 也跑在 `python -m mftik.pack` 打包、再由釘住的 mftiko `internal/platform/artifact` 解開的 rootfs 上（E05-02）：STS 跑的是 rootfs，不是 venv。
- E05-D004 範例策略三個，放在 `examples/python/`：`md_watch`（只用 MD）、`maker`（TD 下單撤單）、`stateful`（volume 與 inputs）。每個都附端對端測試，在 CI 跑，也都能打包。
- E05-D005 使用文件用英文，放在 repo 的 `docs/python/`，Markdown，不架網站。
- E05-D006 #423（hold 滿而 FAILED 時交付 TD buffer）併進 E05，成為 E05-05，在 1.0.0 之前修掉。
- E05-D007 testnet 實機部署（E05-06）等 mftiko #385 的演練完成；由 Yi Te 在 testnet 執行，或開放存取後由 Claude 執行，開工前再定。先只用 MD（`md_watch`），TD 等有 testnet 帳號再加。
- E05-D008 1.0.0 發佈放進 E05，成為 E05-07，在 E05-06 之後，等 Yi Te 確認才打 tag（同 E04-D065）。
- E05-D009 E05-06 時把 pin 改到 testnet 部署的 mftiko commit（`scripts/sync-proto`），並以那個 commit 的 `mftiko-conformance` 重跑 E05-01、E05-02。
- E05-D010 並行與決策區段：E05-01、E05-03、E05-04、E05-05 同時開工；E05-02 等 E05-01；E05-06 等 E05-01–E05-04 與 mftiko #385；E05-07 等 E05-06。各票的決策區段：E05-01 從 D011 起，E05-02 從 D021 起，依此類推，E05-07 從 D071 起。第一張合併的 PR 把 E05-D001–E05-D010 寫進 `docs/DECISIONS.md`；後合併的 PR 以 merge main 解 DECISIONS 衝突，不 rebase。
- E05-D011 wheel 只建一次 abi3 wheel 並存成 artifact；3.11、3.12、3.13 三個 job 各自裝進乾淨 venv 跑 `mftiko-conformance`。
- E05-D012 不另寫 Python conform 策略本身的測試；conformance 全套 11 項即其測試。
- E05-D013 本 PR 把 E05-D001–E05-D010 一併寫進 `docs/DECISIONS.md`；其他平行票以 merge main 解衝突。
- E05-D014 三個 Python conformance job 設為 branch protection 的 required check，由 Yi Te 在 repo 設定加入；PR 不改 repo 設定。

E05-01 的 Python 標準策略在 `examples/python/conform`，以 `python -m mftik.run conform:Conform` 啟動。行為與 `examples/conform` 及 Go `examples/strategies/conform` 一對一：`InitPayload.params` 的欄位同名，沒給的是零值，不認得的略過；`until_seq` 解得開但策略不讀。stdout 每行一個 JSON，欄位與順序同 Go 的 `observation`（`hook`、`ctx`、`seq`、`incarnation`、`view`、`fetch`），零值與空的省略，bool 設了就寫（含 false）。§9.8 列的 `md`、`queued` 不是這支策略寫的（E03-D101）。CI 的 `python-conform-wheel` job 建一次 abi3 wheel，artifact 名 `mftik-abi3-wheel`；`mftiko-conformance-python` 在 3.11、3.12、3.13 並行，各自裝進乾淨 venv，報告 artifact 是 `mftiko-conformance-python-<版本>`。釘住的 `mftiko-conformance` 會換掉 worker 的環境、不轉發 `PYTHONPATH`，所以 venv 的 `mftik-conform.pth` 把 `examples/python` 加進 `sys.path`；argv 仍是 `<venv>/bin/python -m mftik.run conform:Conform`。不另寫這支策略的測試（E05-D012）。branch protection 要加的 check 名稱是 `mftiko-conformance-python (3.11)`、`mftiko-conformance-python (3.12)`、`mftiko-conformance-python (3.13)`，由 Yi Te 在 repo 設定加入（E05-D014）。Rust 版 `mftiko-conformance` job 照跑。本票沒有改 SDK。同一顆 abi3 wheel 在 Python 3.11、3.12、3.13 上，`flow`、`beat`、`deadline`、`shutdown`、`buffer_priority`、`prehook`、`td_overflow`、`cloid`、`subject`、`recon`、`ctx` 全部 pass，沒有 skip。

## 決定（2026-10-26，E05-D021–E05-D025）

Yi Te 於 #441 確認。E05-02 讓 conformance 跑在打包後的 rootfs 上。與 Go 版語意不同之處先回報再做。

- E05-D021 rootfs 以 `mftik.pack` 的 3.11、3.12、3.13 三版各打包一次，並行跑 conformance。
- E05-D022 另開 `conformance-rootfs` job（三版 matrix），原 `pack` job 不動。
- E05-D023 wheel 用 E05-01 的 `mftik-abi3-wheel` artifact，不另建。
- E05-D024 跑時 `PATH` 只留 conformance 工具目錄，並檢查 entrypoint 是 rootfs 內的路徑，證明不依賴主機 Python。
- E05-D025 合併後由 Grok Bot 把三個新 job 加進 main 的 required check；PR 不改 repo 設定。

E05-02 的 `conformance-rootfs` job 在 3.11、3.12、3.13 並行（`fail-fast: false`），`needs: python-conform-wheel`。每個 job 下載 artifact `mftik-abi3-wheel`，不重建 wheel，只把它裝進乾淨 venv 以執行 `python -m mftik.pack`。`-dir` 是 `examples/python`（裡面是套件 `conform`），`-target conform:Conform`，`-python` 是該 job 的版本，`-wheel` 是那顆 abi3 wheel，沒有額外依賴。映像推到本機 `127.0.0.1` 的 `registry:2`，repository `mftik/conform`。接著用 E04-D076 釘住的 mftiko commit `dd73a2d421ca` 的 `internal/platform/artifact`（`tests/pack/check`）依 PROTOCOL §8.3 拉取、檢查、解開。manifest 的 entrypoint 必須是 `python/bin/python3 -m mftik.run conform:Conform`。第一段不得以 `/` 開頭、不得含空段、`.` 或 `..`；詞法路徑在 rootfs 內，而且 symlink 的目標也不得離開 rootfs，這樣才不是主機上的 Python。`mftiko-conformance -json report-rootfs.json -- <entrypoint>` 的工作目錄是 rootfs，argv 就是這份 manifest entrypoint。這個行程的 `PATH` 只有 `mftiko-conformance` 所在目錄，`HOME` 是空目錄，不轉發 `PYTHONPATH` 與 `LD_LIBRARY_PATH`。釘住的 harness 會換掉 worker 的環境，只留下 `PATH`、`HOME`、`TMPDIR`，不套用 manifest 的 `PYTHONPATH`（與 E05-01 相同）。解開之後在 rootfs 的 `python/lib/python<版本>/site-packages/mftik-conform.pth` 寫入 manifest `PYTHONPATH`（`app:site-packages`）相對該目錄的路徑，讓 `import conform` 找得到策略。這份檔不在 image 裡；STS 正式啟動時是把 manifest 的環境放進 worker。報告不論成敗都上傳，artifact 名 `mftiko-conformance-rootfs-<版本>`。11 項 `flow`、`beat`、`deadline`、`shutdown`、`buffer_priority`、`prehook`、`td_overflow`、`cloid`、`subject`、`recon`、`ctx` 必須全部是 pass；工具把 skip 當成功，腳本看到 skip 或 fail 就讓 job 失敗。branch protection 要加的 check 名稱是 `conformance-rootfs (3.11)`、`conformance-rootfs (3.12)`、`conformance-rootfs (3.13)`，合併後由 Grok Bot 加入（E05-D025）。本票不改 repo 設定、不改 `pack` job、不改 SDK、不改 PROTOCOL。不推送任何對外的 image 或 tag。

## 決定（2026-10-26，E05-D031–E05-D035）

Yi Te 於 #442 確認。E05-03 範例策略。與 Go 版語意不同之處先回報再做。

- E05-D031 每個範例的 e2e 測試放在該範例目錄內（例如 `examples/python/maker/test_e2e.py`）。
- E05-D032 另開 `examples` CI job，用 `mftik-abi3-wheel` 跑三個範例的 e2e，並各以 `python -m mftik.pack` 打包一次（不推送）；合併後由 Grok Bot 加進 required check，PR 不改 repo 設定。
- E05-D033 三個範例都從 `InitPayload.params` JSON 讀 symbol、下單量等參數，不寫死交易所或商品。
- E05-D034 `stateful` 存一個計數器（例如處理過的成交筆數），在計時器觸發與 OnStop 時原子寫入 volume；測試涵蓋「有 volume 接續」「只有 inputs」「都沒有從零開始」三種情況。
- E05-D035 README 用英文（同 E05-D005）。

E05-03 的三個策略在 `examples/python/md_watch`、`examples/python/maker`、`examples/python/stateful`。各目錄的 `test_e2e.py` 寫場景，再跑 `cargo test -p mftik-py --test examples -- --ignored --exact drive`。被忽略的測試在 `crates/mftik-py/tests/examples.rs`，用 `mftik-testkit` 的假 shim、nats-server，以及假的 MD、TD、SYM 對端。`cargo test --workspace` 會編譯這個檔、不執行它。CI 的 `examples` job `needs: python-conform-wheel`，下載 artifact `mftik-abi3-wheel`，不重建 wheel，裝進乾淨 venv 後跑三個 `test_e2e.py`。接著各以 `python -m mftik.pack -no-push` 打包一次：映像會建好並印出 digest，不連 registry。`-repo` 仍必填（`build_image` 要拆 host 與路徑），這個值不會被連線。`-no-push` 是「不推送」的做法，不是新的決定編號。`after` 每次呼叫是一個計時器，範例在回呼裡再排下一次；這是既有的 SDK 行為。branch protection 要加的 check 名稱是 `examples`，合併後由 Grok Bot 加入（E05-D032）。本票不改 repo 設定、不改 PROTOCOL。不推送任何對外的 image 或 tag。

## 決定（2026-10-26，E05-D051–E05-D054）

Yi Te 於 #423 確認，並於同日修訂 E05-D051（對齊 Go `sdk/internal/tdpath`，不改 runtime）。

- E05-D051（修訂）照 Go：`onEvent` 在 `failed` 時直接返回。hold 滿而 FAILED 之前已排入並套用的 live 事件，OnStop 的 OMS／Ledger 都含。觸發 FAILED 之後才處理到的事件，即使序號接得上也丟掉，不扣住、不發請求。
- E05-D052 hold 裡已扣住的事件不交付（`release_step` 見 failed 即停）。
- E05-D053 觸發 FAILED 的那一筆（第 65537 筆）不交付、不進 hold。
- E05-D054 failed 之後的 order 與 recon 回覆不處理（`fail_full` 清掉 rounds；`on_recon_round` 見 failed 即返回）。

Rust 在 dispatcher 取出時才分類（E03-D071）。第 65537 筆之前的事件在 FIFO 裡更早，`failed` 之前就套用，所以 OnStop 看得到。第 65537 筆之後才推進 TD buffer 的事件，Go 在 NATS callback 裡已經看到 `failed` 而丟掉；Rust 在取出時看到 `failed`，同樣丟掉。現行行為與 Go 等價，程式不改。測試在 `mftik-core`（sans-IO，時鐘是注入的 `NOW_MS`）與 `mftik-rt`（真實 nats-server；dispatcher 的 `on_stop` 在 TD drain 之後讀 OMS／Ledger，與策略的 OnStop 同一點）。沒有改 PROTOCOL。

## 決定（2026-10-26，E05-D041–E05-D045）

Yi Te 於 #443 確認。E05-04 使用文件（`docs/python/`）。與 Go 版語意不同之處先回報再做。本票沒有改 PROTOCOL。

- E05-D041 先寫不引用範例的章節；#442 合併後 merge main 補上範例片段，只開一個 PR。
- E05-D042 文件內每段程式碼標註來源檔與行號，CI 腳本比對片段與範例檔一致。
- E05-D043 CI 腳本以 `mftik` 的公開名稱（`__all__` 與 `.pyi`）比對文件，漏寫即失敗。
- E05-D044 API 參考手寫 Markdown，不由 docstring 產生、不架網站（同 E05-D005）。
- E05-D045 兩支檢查腳本放在現有 `python` job，不另開 job。

使用文件在 `docs/python/`，英文，README 連到該目錄。程式碼區塊只來自 `examples/python`，info string 是 `<language> <path>:<start>-<end>`（行號從 1 起，含尾）。`scripts/check-python-doc-snippets` 把區塊本文與那些行逐行比對；沒有區塊時通過。`scripts/check-python-doc-api` 收集手寫的 `__all__` 與手寫 `.pyi` 的公開定義（dunder 算，單底線只在 `__all__` 裡才算）。產生的 `common/`、`md/`、`td/` stub 排除（E04-D021）；文件寫對應規則與簽章型別，不逐欄手抄產生碼。每個名稱必須以完整單字出現在行內程式碼或區塊本文。兩支腳本在 `python` job、`maturin build` 之前跑。#450（`94f26533`，#442 的範例）合併進 main 之後，本 PR 以 merge main（不 rebase）補上範例片段。區塊標的是 `examples/python/md_watch/md_watch.py`、`examples/python/maker/maker.py`、`examples/python/stateful/stateful.py`，以及 `md_watch` README 的啟動與打包命令。

## 決定（2026-10-27，E05-D061–E05-D065）

Yi Te 於 #444 確認。E05-06 testnet 實機部署。本票沒有改 PROTOCOL。

- E05-D061 pin 改到 testnet 跑的 mftiko v0.2.1（`7a8c64493109`）。`scripts/sync-proto`（proto 與 `mftiko-conformance`）和 `tests/python/conformance_rootfs.py`、`tests/python/pack_e2e.py` 的 `MFTIKO_COMMIT` 一起改，取代 E01-D004 與 E04-D076 的 commit。
- E05-D062 由 Claude 經 testnet 的 API（`mftiko.lynxlinkage.com`）與 `mftiko` CLI 執行，不重架環境（E05-D007）。worker 的 log 在 STS 主機上，由 Yi Te 讀取。
- E05-D063 image 推到 `ghcr.io/lynxlinkage/strategies/<name>`。SDK 用 main 的 CI 建的 abi3 wheel（`-wheel`），因為 PyPI 上的 0.14.0 早於 `mftik.pack`。在 `linux/amd64` 容器內打包。
- E05-D064 v0.2.0 上結束後的狀態沒有 `last_exit`（BullionBear/mftiko#433），第一次執行先以 log 的 `OnStop` 行記錄 OnStop 已完成。#433 在 v0.2.1 修好後重跑，以 API 的 `last_exit.on_stop_completed: true` 勾完成標準。
- E05-D065 session 檔放在 `examples/python/md_watch/session.yaml`，推送與 `mftiko` 命令寫在同目錄的 README，`docs/python/pack.md` 的「Deploy a session」照 E05-D042 引用它們。

第一次執行的 session 是 `2407bf`（2026-10-27，`sts-jp-1`、`md-jp-1`，image `sha256:0037ffce4ec7…`）。Init、Start、running 都正常，收到 330 筆 `OnBook`，`session end` 後 OnStop 跑完，record 的 `final` 是 `done`/`ended_by_spec`，紀錄貼在 #444。執行中發現的兩個 mftiko 問題：建 session 只能用 API 設定列出的 repository（BullionBear/mftiko#429，已由 v0.2.0 的 #431 修好，改為 `{repository, ref}` 加前綴允許清單）；`session end` 被當成 spec 變更，多 spawn 一個 incarnation，終態沒有 `last_exit`（#433，v0.2.1 修好）。

第二次執行是 v0.2.1 上的 session `6dfca4`（同一個 image 與 session 檔）。`session end` 之後狀態是 `done`/`ended_by_spec`，`incarnation` 1，`last_exit` 為 `outcome` `done`、`code` 0、`on_stop_completed` true，沒有多 spawn。

`pack.md` 原本說 macOS 能打包，但 packer 要執行釘住的直譯器來編 bytecode，在非 linux x86_64 的機器上，本機 Python 必須是同一個 patch 版本，文件已改正。
