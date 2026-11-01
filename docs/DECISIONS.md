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

### E06 跟進 mftiko

範圍：1.0.0 之後跟上 mftiko 的改動：pin 移到 testnet 的 release、文件跟上新規則，以及 mftiko 指定由 mftik 守的 Python 端測試（例如 mftiko E15-D010）。一張票一段決策，從 E06-D001 起依序編。

依賴：E05。

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
- E04-D008 發佈（原第 5 題）：版本 0.14.0。PyPI 已有舊 SDK 的 0.12.0、0.12.1，git 有 v0.13.0-a1–a4；1.0.0 留到 E05 實機跑過。取代 E01-D003 的「版本從 0.12.0 起」。wheel 出 manylinux_2_28 x86_64（必須）與 macOS arm64（本機開發、型別檢查與 `python -m mftik.pack` 用，不保證能跑 session），不出 sdist。PyPI 用 trusted publishing，PyPI 專案 owner 要設定一次。發佈工作是 E04-06；在那之前這棵樹的套件版本維持 0.12.0，不在 E04-01 改 workspace 版本（那會改到 `mftik-rt` 的 `RELEASE`）。（「在那之前這棵樹的套件版本維持 0.12.0」由 E04-06 結束，版本改為 0.14.0。）（「wheel 出 manylinux_2_28 x86_64」已由 E06-D018、E06-D019 修改：Linux wheel 改為 manylinux_2_35 x86_64，支援 Ubuntu 22.04 以上。）
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
- E04-D063 每個 PR 的 CI 建 manylinux_2_28 x86_64 與 macOS arm64 兩個 wheel，各裝進乾淨 venv 跑 smoke。（manylinux_2_28 x86_64 已由 E06-D019 修改：改為在 `ubuntu-22.04` 上建的 manylinux_2_35 x86_64。）
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

## 決定（2026-10-27，E05-D071–E05-D073）

Yi Te 於 #445 確認。E05-07 1.0.0 發佈。本票沒有改 PROTOCOL。

- E05-D071 版本改成 1.0.0：`Cargo.toml` workspace version、`pyproject.toml`、`tests/python/smoke_import.py`、`python/mftik/_version.py`，以及 `Cargo.lock` 的 workspace 成員與 `uv.lock` 的 `mftik`。`tests/python/pack_e2e.py` 比對安裝的 SDK 版本（`sdk_version`、`MFTIK_RELEASE`），跟著改。
- E05-D072 classifier 改成 `Development Status :: 4 - Beta`，修改 E04-D064 的 Alpha；`scripts/release-check` 的 `CLASSIFIERS` 跟著改。其餘 metadata（3.11–3.13、`requires-python >=3.11`、abi3-py311）與兩個 wheel 平台不變。
- E05-D073 不跟著版本改的：`tests/python/test_pack.py` 的 `0.14.0` 是測試資料（其中一處刻意與 fixture 的 1.0.0 不符）；`examples/python/md_watch` 的 image tag `v0.14.0` 與 digest 是 E05-06 在 testnet 實際推送的紀錄。

PR 合併後，等 Yi Te 確認，在 main 打 `v1.0.0`，由 `release.yml` 以 trusted publishing 發到 PyPI（同 E04-D061、E04-D065）。`uv.lock` 只改 `mftik` 那一筆的版本，不以本機較舊的 uv 重寫整份（那會把 `revision` 從 5 降成 3）。

## 決定（2026-10-27，E06-D001–E06-D003）

Yi Te 於 #455 確認。打包後的 Python 策略讀資料 input（mftiko E15-06，BullionBear/mftiko#451，依 mftiko E15-D010）。本票沒有改 PROTOCOL。前一張 #456（pin 改到 v0.3.0、`session.md` 的 inputs）沒有新決定。

- E06-D001 只跑一個直譯器（image 的 3.12），不跑 3.11–3.13。input 的路徑與直譯器無關；PyO3 那層由 `mftiko-conformance-python` 在三版各跑一次（E05-D002）。
- E06-D002 寫入被拒由測試行程檢查：worker 結束後，以與 worker 相同的使用者在 input 裡新建檔案、寫入既有檔案，兩者都要 `PermissionDenied`。範例策略不放測試用的程式碼。這需要非 root 的 runner；GitHub 的 `ubuntu-latest` 是 `runner` 使用者。
- E06-D003 範例叫 `pretrained`，input 叫 `model`，檔案是 `weights.json`。它是第五個範例目錄（含 `conform`），有自己的 `test_e2e.py`、README 與 `session.yaml`，`examples` job 跑它的測試並打包（E05-D004）。

`tests/python/data_input_e2e.py` 由新的 `data-input` job 執行：以釘住的 `mftiko-pack data` 推兩次並要求同一個 digest（E15-D008）；`tests/pack/data/main.go` 以 `PullData`、`UnpackReadOnly` 解到 `sessions/s1/inputs/model`，與 `internal/sts/agent/inputs.go` 相同；檢查 config 沒有 `source`（E15-D003），內容與上傳逐位元組相同、檔案 0444、目錄 0555；再以 `MFTIKO_INPUT_DIR` 經假的 shim 跑 rootfs（`crates/mftik-py/tests/pack.rs` 的 `unpacked_rootfs_reads_a_data_input`）。這支腳本從 `pack_e2e.py` import pin 與 registry 的 helper，不再多一份 `MFTIKO_COMMIT`。`UnpackReadOnly` 依賴 Linux 允許在同一個父目錄內 rename 唯讀目錄，macOS 上最後的 rename 會失敗；STS 只跑 Linux，CI 也是 Linux。

## 決定（2026-10-28，E06-D004–E06-D010）

Yi Te 於 #459 確認。自寫 log 庫與 Python logging API，格式對齊 BullionBear/mftiko `pkg/logger` 與 `sdk/log.go`（553ece5，E15-D017–D022）。mftiko 本身不改。本票沒有改 PROTOCOL。

- E06-D004 自寫 log 庫。Rust 端日誌全部改寫成自己的 log 模組，放在 `mftik-rt`。不用 zerolog 的 Rust crate，也不用 `tracing` 或 `log`。輸出與 Go zerolog 相同：一行一個 JSON，鍵為 `level`、`time`（UTC RFC3339Nano）、`component`、`version`、`commit`、`sdk_version`、（`session_id`）、其他欄位、`message`。mftiko Go 照舊用 zerolog。
- E06-D005 Python API 比照 Go。`ctx.log` 帶 `session_id`，給 hook 內用；`mftik.log` 是模組層級，不帶 `session_id`，給 hook 外用。提供 `debug`、`info`、`warn`、`error`，寫法是 `ctx.log.info("msg", key=value, ...)`。不接 Python 標準的 `logging`。
- E06-D006 欄位值型別。只接受 str、int、float、bool、None，其他型別丟 `TypeError`，不自動轉字串。（已由 E06-D017 修改：只有會寫出的那一行才檢查；低於 level 的呼叫直接返回，不丟 `TypeError`。）
- E06-D007 寫出方式。在 Rust 端序列化，釋放 GIL，每行一次 `write` 到 stderr，同步寫，不排隊也不丟行。
- E06-D008 版本欄位。`build.rs` 用 vergen 把 mftik 的版本與 commit 寫成 `sdk_version`。策略的 `version`、`commit` 由 `mftik.run()` 的選填參數傳入，不填就是空字串。`component` 是 `strategy`。
- E06-D009 level 與 format。讀 `MFTIKO_LOG_LEVEL`（預設 info）與 `MFTIKO_LOG_FORMAT`，`console` 格式也支援，和 Go 一樣。
- E06-D010 範圍。`note()` 的 `mftik sdk:` 輸出與 Python 範例裡的 `_log()` 都改用新模組。conformance 的 `{"hook":"gate"}` 照舊留在 stdout。加 benchmark，只報數字，不和 Go 比。

`sdk_version` 是套件版本、`+`、完整 git sha，由 vergen-gitcl 在 `mftik-rt` 的 `build.rs` 寫入（vergen 9 起 git 後端拆成這個 crate）。manylinux 容器裡 git 若不把 checkout 當成 worktree，同一個 sha 仍寫入：先 `git -c safe.directory=* rev-parse HEAD`，否則用 `GITHUB_SHA`。vergen 在失敗路徑讀 `VERGEN_GIT_SHA`，不改成 idempotent 的佔位字串。`mftik.run` 的 `version` 與 `commit` 是 keyword-only，預設空字串，不取代 `Hello.release`（`release` 仍走 `resolve_release`）。`note()` 的 message 不再加 `mftik sdk:` 前綴，與 Go `plane.note` 相同。範例裡本來以 list 寫出的欄位（maker 的 open client order id、pretrained 的 weights keys）改成逗號串起來的 str，因為 E06-D006 不接受 list。

## 決定（2026-10-28，E06-D011–E06-D016）

Yi Te 於 #461 確認。Rust 一行 JSON log 的成本對照 Go zerolog（#459 的後續）。本票沒有改 PROTOCOL。

- E06-D011 JSON path reuses one thread-local buffer per thread; rendering a JSON line performs zero heap allocations; the line is written directly to stderr.
- E06-D012 Integers are formatted with the `itoa` crate, floats with the `ryu` crate; float output is adjusted to match Go zerolog's output exactly, with tests covering the cases (including large/small magnitudes, NaN/Inf handling as zerolog does).
- E06-D013 Timestamp: cache the formatted date-time prefix for the current second; per line only the nanosecond part is written by hand.
- E06-D014 Acceptance: on the same machine, a JSON line is no slower than Go zerolog with the same fields. A test using a counting allocator asserts zero allocations per JSON line. CI enforces no timing threshold.
- E06-D015 Exception to E06-D010's no-comparison rule for this ticket: the PR reports Rust and Go zerolog numbers side by side (same fields, same machine), plus the end-to-end cost of `ctx.log.info(...)` called from Python.
- E06-D016 Scope: only the JSON output is optimized; the console format stays as it is. Log format, key order and E06-D004–D010 semantics are unchanged.

每條執行緒的 JSON 緩衝一開始是 1 KiB；某一行讓它超過 64 KiB 時，那一行寫完就釋放，下一行重新配置（zerolog 的 event pool 也不收超過 64 KiB 的）。緩衝正在用（sink 裡又寫 log）或執行緒已經釋放它時，那一行用一個新的緩衝。`emit` 照舊只在 render 時拿 identity 的讀鎖，寫之前放開，寫法仍是 `io::stderr().lock().write_all`，一行一次 `write`（E06-D007）。`note()` 的 session id 改存 `Arc<str>`，SDK 自己的行也不配置。ryu 與 Go 的文字差在三處，各自改寫：整數多一個 `.0`、正指數沒有 `+`、ryu 在 `[1e-5, 1e16)` 之外改用指數而 Go 的 `'f'` 是 `[1e-6, 1e21)`；ryu 的指數本來沒有前導零，就是 zerolog 的 `e-X`。#460 的 JSON 用 Rust 的 `{}`：兩個最短寫法一樣近時（例如 `1245315403051669.25`）它取離零遠的一個，Go `strconv` 取偶數，所以那種值的最後一位與 Go 不同；現在 JSON 與 Go 相同。console 照 E06-D016 不動，那種值的最後一位因此與 JSON 不同。`log.rs` 的浮點、字串表由 Go zerolog v1.35.1（mftiko 釘住的版本）產生，時間表由 Go `time.Format(time.RFC3339Nano)` 產生。`tests/log_alloc.rs` 的 global allocator 依執行緒計數：每條執行緒先寫一行建立緩衝，之後 `Logger::write` 1000 行（跨過幾個秒）與經 pipe 寫到 fd 2 的 `emit` 1000 行都是 0 次配置。另在本機以 Go zerolog 對照約 595 萬個值（splitmix64 的位元型樣、1e-6、1e-5、1e16、1e21 前後各兩萬個 ulp、短小數、整數、2 的冪），JSON 全部相同；這份對照不進 CI。benchmark 多一個 `json_line_clock`，每行讀牆鐘，與 zerolog 的 `Timestamp` 相同；兩邊的數字只寫在 PR（E06-D015）。

## 決定（2026-10-28，E06-D017）

Yi Te 於 #461 確認，修改 E06-D006。與 E06-D011–E06-D016 同在 #462 實作。本票沒有改 PROTOCOL。

- E06-D017 `ctx.log.*` / `mftik.log.*` check the configured level first. A call below the level returns immediately: no field conversion, no GIL release. Field type validation (`TypeError`, E06-D006) applies only to lines that are emitted, matching Go zerolog's behavior for filtered events. This amends E06-D006. Implemented in PR #462 together with E06-D011–D016; the PR reports the filtered-call cost.

binding 的 `emit` 第一步呼叫 `mftik_rt::log::enabled(level)`，它讀的是 `emit` 用的同一份、只取樣一次的 `MFTIKO_LOG_LEVEL`。低於 level 時直接回 `None`：不檢查 message 是不是 `str`、不轉欄位、不複製 session id、不釋放 GIL；`MFTIKO_LOG_LEVEL=disabled` 時每個呼叫都是這樣。Python 自己綁定參數的錯誤照舊，例如少了 message 仍是 Python 的 `TypeError`。`ctx.log` 這個 getter 不變，hook 外讀它仍丟 `ContextError`（E04-D005），與之後呼叫的 level 無關。`docs/python/context.md` 與 `reference.md` 跟著改。`src/log.rs` 與 `hook_tests.rs` 的測試在預設 level（info）下檢查：`debug` 帶不合的 message 與欄位回 `None`、不寫出，`info` 帶同樣的欄位仍丟 `TypeError`。`tests/python/test_log.py` 以 `MFTIKO_LOG_LEVEL=warn` 與 `disabled` 起子行程檢查同一件事，且只寫出 level 以上的行。`hook_tests.rs` 的 `ctx_log_cost` 另外量低於 level 的 `ctx.log.debug(...)`，數字寫在 PR。

## 決定（2026-10-31，E06-D018–E06-D021）

Yi Te 於 #465 確認，修改 E04-D008、E04-D063 的 Linux wheel。起因是 2026-10-28 main 的 ci 因 quay.io 502 失敗（run 40676834922），以及 #451 的 sym 測試 flake。本票沒有改 PROTOCOL，也不改套件版本。

- E06-D018 支援平台只有 Ubuntu 22.04 以上的 x86_64 與 macOS arm64。不再出 manylinux_2_28 或 Linux aarch64 的 wheel。
- E06-D019 Linux wheel 直接在固定的 `ubuntu-22.04` runner 上 `maturin build --compatibility manylinux_2_35`，不用 `ubuntu-latest`，不用容器，不拉 quay.io。標籤是 `manylinux_2_35_x86_64`，PyPI 收這個標籤。macOS arm64 照舊。
- E06-D020 `ci.yml` 與 `release.yml` 同步改；文件中寫到支援平台的地方一起更新。
- E06-D021 `crates/mftik-rt/tests/sym.rs` 的 `fake sym connects` 偶發 Connection refused（#451，run 40674529066）依根因修正，不以加長等待或重試掩蓋。

`.github/actions/wheel` 在 Linux 用 setup-python 的 3.11 從 PyPI 裝 maturin 1.15.0，Rust 與其他 job 一樣由 `dtolnay/rust-toolchain` 裝（版本照 `rust-toolchain.toml`），在 runner 上直接 build；之前先檢查 `getconf GNU_LIBC_VERSION` 是 `glibc 2.35`，標籤就是這台 runner 的 glibc。maturin 依 manylinux_2_35 的 policy 檢查 extension：要用比 2.35 新的 glibc 符號，或連到 policy 以外的函式庫，就不出 wheel。macOS 照舊用 maturin-action（macOS 上它本來就不起容器）。兩個 workflow 的 matrix 拿掉 `target` 與 `manylinux`，release 的 artifact 改名為 `wheel-manylinux_2_35_x86_64`。`scripts/release-check` 的 Linux 平台改為 `manylinux_2_35_x86_64`，wheel 檔名的平台標籤必須正好是它。PyPI 這邊：warehouse 的 `_linux_platform_re` 收任何 `manylinux_<major>_<minor>_x86_64`，PyPI 上也已有這個標籤的 wheel（例如 `rerun_sdk-0.2.0-cp38-abi3-manylinux_2_35_x86_64.whl`）；本票沒有上傳任何東西。pip 的 `--platform` 對 PEP 600 標籤是逐字比對，所以 packer 下載 SDK wheel 時列 `manylinux_2_35_x86_64` 與 `manylinux_2_28_x86_64`（1.1.0 以前的版本）；依賴層照 E04-D072，仍是 manylinux_2_28。`mftik-rt` 的 `build.rs` 保留 `safe.directory` 與 `GITHUB_SHA` 的後備，註解不再說 wheel 在容器裡建。

E06-D021 的根因：測試先 bind `127.0.0.1:0` 取得 port、關掉，再把 port 交給 nats-server，然後重試 TCP connect，連上就當作 nats-server 已經 listen。libtest 在多條執行緒上同時跑測試；另一條執行緒在 bind 與 close 之間 `Command::spawn` 出的子行程，在 exec 之前握著那個 listening socket 的複本（close-on-exec 到 exec 才關）。測試執行緒 close 之後 socket 仍在 LISTEN，探測的 connect 由它接受，於是在 nats-server listen 之前就返回，假 SYM 的 `async_nats::connect` 接著被拒。本機把 bind 到 close 與所有 `Command::spawn` 以同一把鎖序列化，sym 測試 150 次 0 失敗；不加鎖交錯跑的 150 次失敗 8 次，都是這一行。修法：nats-server 自己選 port（`-p -1`，設定檔 `listen: 127.0.0.1:-1`），`mftik_testkit::NatsServer` 從它 listen 之後印的 `Listening for client connections on 127.0.0.1:<port>` 讀出 port，之後繼續讀 stderr，pipe 不會塞住。`start_nats`、`tests/sym.rs`、`src/md/tests.rs`、`src/td/tests.rs` 都改用它，`free_port` 刪除。同一檔的 `a_reconnect_compares_revisions_at_once` 另有一個時序問題：pass 先存 row（tick 已變），才送收尾的 Revisions，所以比對呼叫序列之前改為等假 SYM 收到第四個呼叫。根因說明與連跑次數寫在 PR。

## 決定（2026-10-31，E07-D001–E07-D006）

Yi Te 於 #468 核可。策略事件紀錄，跟 BullionBear/mftiko E20（BullionBear/mftiko#499，squash `5fbfd14`，PROTOCOL §12 v1.91，E20-D001–E20-D010）。

- E07-D001 寫入核心用 Rust 實作於 `mftik-rt`（writer thread + ring，不碰 GIL），結構對應 Go SDK。
- E07-D002 不另寫 Python dump，沿用 `mftiko-eventlog dump`；mftik 檔案必須逐 byte 符合 PROTOCOL §12。
- E07-D003 檔頭 `sdk_version`/`sdk_commit` 寫 mftik 自己的版本與 commit。
- E07-D004 `ctx.rand()` 回傳以 init seed 建立的 `random.Random`；`ctx.now()` 回傳 int ns（hook 開始時間），與 Go 一致。
- E07-D005 replay 範圍對齊 Go §12.6，phase 1 不比對 log。
- E07-D006 E07-05 完成後升 1.3.0；tag 由 Yi Te 決定，PyPI 由 Yi Te 上傳。

#469（E07-01）把 proto 與 conformance 的 pin 改到 `5fbfd14`：`SessionSpec.event_log`、`InitPayload.event_log`（欄位 12）、`Report.event_log`（欄位 6，`EventLogStat` 的 `records`、`dropped`）。Init 只在 `event_log.enabled` 為真時把它記在 session 上；`Report.event_log` 只在那時有，與 Go `eventLogStat` 相同。本票還沒有 writer，不寫檔，所以開啟時是空的 `EventLogStat`（兩個計數都是 0，編碼為 `32 00`）。關閉（沒有 `event_log`，或 `enabled` 為假）時 Report 不帶這個欄位，編碼與舊 pin 逐 byte 相同；`report/tests.rs` 以舊 pin 下產生的 bytes 檢查。`keep_mb` 留給 E07-02 的 writer。

#470（E07-02）在 `mftik-rt` 加 `eventlog` 模組：檔案格式、writer 與測試用的 reader，結構對應 Go `sdk/eventlog` 與 `sdk/internal/evlog`（E07-D001）。本票不接收錄點，session 還不開 writer，`Report.event_log` 照舊是空的。`Log::append` 在一把鎖裡把 40 bytes 框頭與 body 複製進預先配好的 8 MiB ring 就返回，不配置、不碰磁碟；ring 放不下、框比 ring 或 64 MiB 大、已關閉時丟掉並計數，`rec_seq` 照樣用掉。writer 執行緒每 100 ms，或 ring 用到 1/4 時被叫醒，在鎖外把 `[head, tail)` 寫進檔案，寫完才移動 `head`，所以 append 寫的範圍與它讀的範圍不重疊。輪替、保留（`keep_mb` 0 是 256 MiB，單檔是保留上限的 1/4，介於 64 KiB 與 64 MiB）、`O_EXCL`、目錄 0750、檔 0640、寫不進去時一秒後再試且同一段失敗只記一行、`close` 寫 `end` 後最多等 5 秒再 fsync 最後一個檔，都照 PROTOCOL §12.1（mftiko v1.92）與 Go 寫入器。檔頭的 `sdk_version` 是套件版本（`RELEASE`），`sdk_commit` 是建置的 git commit（`VERGEN_GIT_SHA`），即 mftik 自己的（E07-D003）。`src/eventlog/tests.rs` 測 §12.1–§12.3，檔頭與框以 Go `AppendHeader`、`AppendFrame` 在釘住的 commit 產生的 bytes 比對。`tests/eventlog_dump.rs` 以 `mftiko-eventlog dump` 讀 writer 寫出的檔，逐行比對 JSON；工具由 `scripts/install-mftiko-eventlog` 從 `contract/proto/SOURCE` 的 commit 安裝，與 conformance 相同，CI 的 rust job 設 `MFTIK_EVENTLOG`，找不到時測試失敗，不跳過。`tests/eventlog_alloc.rs` 以依執行緒計數的 allocator 檢查 10 萬次 append 是 0 次配置。

## 決定（2026-10-31，E07-D007–E07-D009）

Yi Te 於 #471 核可。E07-03 收錄 MD/TD 輸入與 hook 起訖（kinds 5–7、11、12、15），照 PROTOCOL §12.4、§12.5。本票沒有改 PROTOCOL。

- E07-D007 收錄位置：在 Rust dispatcher（mftik-rt）對應 Go runMD/runHook/finishHook 的位置收錄 kinds 5–7、11、12、15，不在 Python 層，熱路徑不多跨 PyO3。
- E07-D008 例外的處理：Python hook 拋出例外或 Rust panic 時，照 §12.5 一樣寫出 finishHook 結束 frame 並標記錯誤，讓每個 hook 起訖成對。
- E07-D009 關閉時的成本與 benchmark：關閉時只多一次 `Option` 檢查，不讀時鐘、不配置，並用測試鎖住。開啟時用 criterion 量每次收錄延遲，結果寫在 PR，與 Go 約 75–127 ns 對照，不設 CI 門檻。

#471（E07-03）的收錄點在 mftik-rt 跑的 dispatcher，即 `mftik-core` 的 `dispatch`（core 不做 IO，E02-D001）：`Config.rec` 是 `mftik_core::eventlog::Recorder`，mftik-rt 的 `eventlog::Recording` 把 E07-02 的 writer 接上，時間讀 session 的 `Clock`（E03-D004）。`run_hook` 對應 Go `runHook` 與 `finishHook`：hook 執行前寫它的輸入，`t_ns` 是 hook 開始的時間；返回後寫 `hook_end`，`recv_ns` 是 0，`t_ns` 是返回的時間，`ref` 是輸入的 `rec_seq`。`run_md` 對應 `runMD`：prehook 留下、有 hook 接的訊息寫 `md_feed` 或 `md_book`，body 是 `md.f` 的訊息 bytes，`recv_ns` 是它進 MD ring 的時間。`timer` 的 `ref` 是計時器編號：`Dispatcher::timer`（`ctx.after`）與 book 的計時器共用一個序列，依建立順序從 1 起編。`post` 的 `ref` 是 `Context::post`、`Poster::post` 依呼叫順序從 1 起的編號，沒排進 TD 的呼叫也用掉一號。`td_event` 的 body 是 `td.evt` 原樣的 bytes，`ref` 是 `api_id`，`recv_ns` 是 reader 收到、還沒解碼的時間；tdpath 把事件的輸入跟著它的 hook 交回（保留的事件放出時也一樣），跑那個 hook 的 TD 項目以 `record_input` 在 hook 之前寫出，`t_ns` 是那個項目開始的時間。Init 在 NATS 連上之後開 writer（Go `openRec`），檔案在 `$MFTIKO_VOLUME_DIR/eventlog`；沒有 volume 時 writer 把每一框都算成丟掉並記一行（§12.1）；writer 執行緒起不來時記一行，session 不帶 log 照跑。`exit_with` 封住 fd 3 之後關 log：寫 `end`，最多等 5 秒，每一種結束都一樣，`HUNG` 也是。`Report.event_log` 改為 writer 的 `records` 與 `dropped`。

E07-D008：hook panic 時 `hook_end` 的 flags 是 1，body 是 panic 的文字，與 Go `EndOf` 相同。Python binding 把 hook 的例外寫成 `型別: 訊息` 再 panic，所以 `ctx.after` 的 callback 丟 `RuntimeError("after broke")` 時，`hook_end` 是 flags 1、body `RuntimeError: after broke`，session 照舊以 `CRASHED` 結束。本票的 hook 都沒有回傳值，只會有 flags 0 與 1；2（error）是 init、start、onstop 回傳的錯誤，屬 E07-04。watchdog 判為 `HUNG` 的 hook 沒有 `hook_end`，之後就算返回也不寫，與 Go 相同；它的輸入在 hook 之前已經寫出。Go 在那時另寫一個 `stop`（`StopHung`），也屬 E07-04。

E07-D009：關閉時每個收錄點是一次 `Option` 檢查。`push_md` 不讀時鐘，MD ring 不存接收時間，TD 佇列不配置與 65536 格平行的輸入陣列，tdpath 保留的事件只多一個空的 `Option<Box<Input>>`，timer 與 post 不編號（都是 0）。`src/dispatch/tests.rs` 檢查關閉時這些都沒有；開啟時以記憶體裡的 recorder 數時鐘讀取：一個 MD 或 post hook 讀三次（進佇列、開始、結束），已帶接收時間的 TD 輸入讀兩次，沒有輸入的 TD hook 只讀開始一次（給 `record_input`）。`tests/hook_alloc.rs` 以依執行緒計數的 allocator 跑 1 萬輪、每輪 5 個 hook（MD、帶輸入的 TD、在 hook 裡才寫輸入的 TD、post、timer），關閉與開啟都是 0 次配置。`benches/hook.rs` 量一個 MD hook 與一個 TD hook 從進佇列到跑完、log 關與開的時間（writer 丟掉輸出），另量讀一次時鐘與 append 一框；與同一台機器上 Go 的對照只寫在 PR。mftik-rt 沒有每個 hook 一個的 watchdog timer，所以沒有 Go 帶 watchdog 的那組對照。

與 Go 不同的地方。套用了、但沒有呼叫策略 hook 的 `td.evt`（不屬本 session algo 的 order 與 fill、沒有內容的事件）不寫；Go 把每一筆套用的事件排成一個 TD 項目，所以它們也有一對 `td_event`、`hook_end`。它們不改策略看得到的狀態，Go 的 `ReplayEvent` 也直接以每一筆記下的事件設定 cursor、不檢查缺號，所以少了它們 replay 不變。book 計時器在 arm 時編號；同一個 key 再 arm 時取代前一次，被取代的編號不會跑。Go 的 `After` 每次是新的計時器，被取代的那個照樣響，寫一對什麼都沒做的 `timer`、`hook_end`。框的時間是 session 的 `Clock`（CLOCK_REALTIME 起點加上 CLOCK_MONOTONIC，E03-D004），Go 讀牆鐘；`end` 與檔頭的 `created_ns` 是 writer 讀的牆鐘。其他 kinds（1–4、8–10、13、14、16–19，含 `md.view`、`td.view` 的回覆）是 E07-04。

測試：`mftik-core` 的 `src/dispatch/tests.rs` 測順序、panic、`HUNG`、編號與關閉時的狀態，`src/tdpath/tests.rs` 測即時與保留事件的輸入；mftik-rt 的 `src/md/tests.rs`、`src/td/tests.rs` 以 nats-server 測 MD 訊息、book 計時器與 `td.evt` 的框，`tests/eventlog_session.rs` 以假的 shim 跑整個 session，含 panic 與 `HUNG`；mftik-py 的 `tests/session.rs` 以安裝的 wheel 測 Python 例外（python CI job）。

## 決定（2026-10-31，E07-D010–E07-D012）

Yi Te 於 #472 核可。E07-04 收錄其餘的 kinds（1–4、8–10、13、14、16–19）與關閉路徑，照 PROTOCOL §12.4、§12.5 與 Go `sdk/record.go`。本票沒有改 PROTOCOL。

- E07-D010 init 去除憑證：init frame 去除的 NATS 憑證欄位與 Go 用同一份清單；加測試確認 dump 內容找不到任何憑證字串。
- E07-D011 call id / read id：由 Rust 端在每個 session 內依序遞增產生，與 Go 一致；下單與讀取在 PyO3 回到 Rust 的邊界收錄，不在 Python 層。
- E07-D012 關閉路徑：StopDeliver、StopHung 照 Go 順序先寫停止 frame 再寫 `end`；on_init/on_start panic 時在 process 結束前同步 flush，flush 設等待上限，不卡住關閉。

另外確認：沒有觸發 strategy hook 的 TD 事件維持不寫（E07-03 的做法不變）。Python 的 `ctx.now()`、`ctx.rand()` 與 replay 不在本票。

#472（E07-04）由 plane 寫 dispatcher 以外的框，對應 Go `outside`、`recordBindings`、`recordResolve`。OnInit、OnStart，以及 Start 失敗後、Run 之前的 OnStop，各是一個輸入（`init`、`start`、`onstop`）：hook 之前寫出，`recv_ns` 是收到的時間，`t_ns` 是 hook 開始；返回後寫 `hook_end`，回傳錯誤時 flags 2、body 是錯誤文字。`init` 的 body 是 Init 當時的 client order id 計數（8 bytes），接著是去掉欄位 9 `nats` 的 InitPayload（E07-D010）：`mftik_core::eventlog::without_field` 照 Go `withoutField` 逐欄複製收到的 bytes，只略過那個欄位，與 Go 去掉的是同一份。`init` 的 `ref` 是 session 的 seed，即 Go `Context.Rand` 用的 seed；本票只產生並記下，`ctx.rand()`（E07-D004）不在本票。`start` 的 body 是收到的 StartPayload。`bindings` 記每一個套用了的 snapshot，不比較新、當作沒事套用的也記：Live 的 body 是 payload；Command 的是它的 `bindings` 欄位原樣的 bytes（Go `fieldBytes`），flags `FROM_COMMAND`。`recv_ns` 是它到 worker 的時間（Command 是 Relay 收到時，不是 command 執行緒取出時），`t_ns` 是套用完的時間。`resolve` 記每一次 `Context::resolve`，不在 OnInit、OnStart 而被拒的也記：flags 是 Go `resolveOutcome` 的分類，body 是 ticker，接著成功時是 `SymbolInfo`（`symclient::info_to_proto`，同 Go `InfoToProto`），失敗時是策略拿到的錯誤文字；`t_ns` 是呼叫，`recv_ns` 是答覆回來的時間。

E07-D011：order 與 read 收在 mftik-rt 的 TD、MD path。Python 的 `ctx.submit`、`ctx.tape` 等經 PyO3 呼叫同一組 `Context` 方法，所以 Python 層沒有收錄的程式。call id 與 read id 各自在 session 內從 1 起編，只在 log 開啟時計數，關閉時都是 0，與 Go 相同。Submit、Cancel、BatchCancel 的每一輪與 RequestRecon 各寫一個 `order_call`：flags 是操作（BatchCancel 的輪加 `FROM_BATCH`），`ref` 是 call id，body 是 `api_id`、Submit 記下的 td.view generation 與涵蓋的 client order id。請求送上連線後寫 `order_publish`，body 是送出的 OrderRequest。每一輪怎麼結束是餵它的 TD 項目的 `order_reply` 輸入，flags 是 Go `evlog.Outcome` 的分類；那一輪不跑 hook（例如被接受的 Submit）時是一個只有輸入的項目，與 Go 寫每一輪的回覆相同。tdpath 套用的 Recon 回覆是套用它的 TD 項目的 `td_view` 輸入，策略有沒有 OnRecon 都寫，`ref` 是 `api_id`，body 是 generation 與 ViewReply；沒有套用的（generation 已過時、path 已失敗）不寫，與不跑 hook 的 `td.evt` 相同。每個 `md.view`、`md.fetch` 請求送出時寫 `read_call`，flags 是讀取的種類，`ref` 是 read id，book 同步時自己讀的 snapshot 也算；怎麼結束是送回它的 TD 項目的 `read_reply` 輸入，帶同一個 read id 與 Go `replyIn` 的結果。

E07-D012：dispatcher 在結束交出任何東西之前寫 `stop`：`recv_ns` 是 loop 停下 MD 的時間，`t_ns` 是 C4 要 OnStop 的時間，DONE 與 FAILED 交出 TD buffer，帶 `STOP_DELIVER`；OnStop 是 `onstop` 輸入，有自己的 `hook_end`。watchdog 判 `HUNG` 時在 Run 返回之前寫 `stop`（`STOP_HUNG`），之後不寫 `onstop`。`end` 照 E07-03 由 `exit_with` 關 log 時寫，所以兩條路徑的順序都與 Go 相同。`outside` 的 hook panic 時，寫完帶 panic 的 `hook_end` 就以 `Log::flush_within` 同步等 writer 把它寫進檔案，最多 5 秒（與 close 的上限相同），逾時記一行照樣往下；C4 在那之後才收到失敗，所以 Done 送出時檔案裡已經有它們。回傳錯誤不 flush（Python 的 `on_init`、`on_start` 例外是回傳錯誤），與 Go 相同，由結束時關 log 寫出。

與 Go 不同的地方。Go 的 `outside` 不 recover panic：寫 `hook_end`、關 log（寫 `end`）後讓 process 帶著 panic 結束。mftik-rt 照舊把 OnInit、OnStart 的 panic 當成失敗（`hook panic: ...`），以 `INIT_FAILED` 結束，所以 panic 時只 flush、不關 log：OnStart 失敗後 C4 還會跑 OnStop，`onstop` 與它的 `hook_end` 接在 panic 之後，最後才是 `end`。OnInit panic 的檔案與 Go 相同（`init`、`hook_end`、`end`）。

測試：`mftik-core` 的 `src/eventlog.rs` 測各 body、`without_field` 與 `field_bytes`，`src/dispatch/tests.rs` 測 `stop`、`onstop` 與 `STOP_HUNG`；mftik-rt 的 `src/td/tests.rs`、`src/md/tests.rs` 以 nats-server 測 order 與 read 的框與編號，`src/plane.rs` 測 `init`、`start`、`bindings`、`resolve`、Run 之前的 `onstop` 與 panic 時的 flush，`src/eventlog/tests.rs` 測有上限的 flush 不會等過頭。`tests/eventlog_session.rs` 以假的 shim 跑整個 session：OnInit、OnStart panic 的框在 Done 送到 shim 時已在檔案裡（拿掉 flush，這兩個測試就失敗），`HUNG` 的 `stop` 以 `mftiko-eventlog dump` 讀出是 `"hung":true`。驗收是 `tests/eventlog_kinds.rs`：測試當平台，另一條 NATS 連線當 SYM、MD、TD；策略在 OnInit resolve、OnStart 讀取、OnRecon 下兩張被拒的單，等每個 callback 都跑過才結束。Go `mftiko-eventlog dump` 讀出 19 種 kind 都在、每個輸入有一個 `hook_end`、每個 call id 有一個 `order_publish` 與一個 `order_reply`、每個 read id 有一個 `read_reply`，dump 輸出與 log 檔的 bytes 都找不到 NATS 憑證（`user_jwt`、`seed`、`inbox_prefix`，E07-D010）。mftik-py 的 `tests/session.rs` 改為預期 `init`、`start` 在前。

## 決定（2026-10-31，E08-D001–E08-D005）

Yi Te 於 #478 核可 E08-D001–E08-D004。純 Rust 策略：用 `mftik-rt` 的 `Strategy` trait 寫，用 mftiko 的 `mftiko-pack -bin` 打包部署，不經 Python/PyO3。E08-D005 是 #479 實作時多出來的，Yi Te 於 #478 確認。本票沒有改 PROTOCOL，也沒有改 mftiko。

- E08-D001 打包：`mftik-rt` 自動把 `mftiko.contracts=` 標記嵌進 binary，沿用 `mftiko-pack -bin`，不另寫 Rust 打包工具。
- E08-D002 目標平台：`x86_64-unknown-linux-gnu`，glibc 2.35（Ubuntu 22.04+），與 Python wheel 一致；不做 musl 靜態版。
- E08-D003 範例與文件：`examples/rust/md_watch`（對應 Python 同名範例）；`docs/rust/`（入門、hook、Context、打包部署）；文件片段以 doctest 編譯檢查。
- E08-D004 發佈：先不上 crates.io，用 git tag 依賴；API 穩定後再議。
- E08-D005 Go build info（已確認 (Yi Te, 2026-11-01)）：只有 E08-D001 的標記還打不了包。釘住的 `mftiko-pack -bin` 讀到標記之後，以 Go `debug/buildinfo` 找 binary 連結的 `github.com/BullionBear/mftiko`，取它的版本（有 replace 時取 replace 的版本）當 `sdk.version`（PROTOCOL §8.2），找不到就拒絕：`artifact: no sdk version in buildinfo: not a Go executable`。所以 `mftik-rt` 在 Linux 的 binary 裡另嵌一份 Go 格式的 build info，寫 `github.com/BullionBear/mftiko` 由 `github.com/lynxlinkage/mftik` 取代，版本是 `v` 加上 mftik 的版本；`sdk.version` 因此是 `v1.2.0`（Python 的打包寫 `1.2.0`）。manifest 的 `runtime`、`sdk.lang` 由 mftiko-pack 寫成 `go`；PROTOCOL §8.2 的 `runtime` 只用於顯示與統計，mftiko 也不讀這兩欄。要寫成 `rust`，得改 mftiko-pack，或照 mftiko E09-D002 由 mftik 自己產生 manifest（與 E08-D001 不合），本票都不做。

#479（E08-01）在 `mftik-rt` 加 `pack` 模組，兩份都在編譯時組出。標記與 Go `sdk/contracts.go` 同格式：`mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1` 加換行，由 `Hello.contracts` 用的 `SESSION_CONTRACTS` 組成，兩邊不會不同（mftiko E09-D035）。build info 是 Go 1.18 起的 inline 格式：32 bytes 的檔頭（`\xff Go buildinf:`、指標大小、flags 2）之後，依序是 uvarint 長度加版本字串 `mftik-rt v1.2.0`，與 uvarint 長度加模組資訊；模組資訊前後是 cmd/go 的 16 bytes `infoStart`、`infoEnd`，中間照 `BuildInfo.String` 寫一個被取代的 `dep`。它放在 `debug/buildinfo` 先找的 `.go.buildinfo` section，16 bytes 對齊，長度補到 16 的倍數，同 Go linker。Mach-O 的 section 名要寫成 `segment,section`，所以 build info 只在 Linux 嵌，macOS 的 wheel 照舊；標記各平台都有。兩份都是 `#[used]` static，`run` 一開始以 `black_box` 讀它們：`#[used]` 只擋編譯器，linker 仍可丟掉沒人參照的 section；Go 也是 `init` 讀標記、linker 參照 build info。本機以 debug、release、fat LTO 加 strip（`codegen-units = 1`、`panic = "abort"`）建的 `conform`，釘住的 `mftiko-pack -bin` 都能打包。Python extension 的 `_run` 也呼叫 `run`，所以 `.so` 裡也有這兩份；Python 的打包不讀它們。

CI 的 `conformance-rootfs-rust` job 在 `ubuntu-22.04` 建 release 的 `examples/conform`，先確認 runner 的 glibc 是 2.35（同 wheel job）。`tests/python/conformance_rootfs_rust.py` 以 E04-D076 commit 的 `mftiko-pack -bin` 推到 127.0.0.1 的 registry，以同一 commit 的 `internal/platform/artifact` 拉下、解開（`tests/pack/check` 多一個 `-layers`，預設仍是 3）。它檢查 manifest：`strategy`、`linux/amd64`、四個 contracts、entrypoint `bin/strategy`、env 空、`runtime` 與 `sdk.lang` 是 `go`、`sdk.version` 是 `v` 加 workspace 版本；rootfs 只有 `bin/strategy`，與建出的 binary 逐 byte 相同。之後照 `conformance_rootfs.py` 跑 `mftiko-conformance`：rootfs 是工作目錄，`PATH` 只有工具所在的目錄，11 項都要 pass。binary 在 22.04 上跑得起來，就表示它不需要比 2.35 新的 glibc（E08-D002）。`pack.rs` 的單元測試檢查標記與 `Hello.contracts` 相同、uvarint 與 Go `binary.PutUvarint` 相同，以及 build info 的檔頭、對齊與補齊。

## 決定（2026-11-01，E07-D013–E07-D015）

Yi Te 於 #473 核可。E07-05 補 `ctx.now()` 與 `ctx.rand()`（E07-D004）。seed 已由 E07-04 產生並寫進 `init` 的 `ref`。本票沒有改 PROTOCOL。Replay 仍是 #474。

- E07-D013 `ctx.now()`：hook 內回傳該 hook 開始時間（int ns，與 dispatcher 同一 session clock），同一 hook 內多次呼叫值相同，replay 可對上；hook 外呼叫回傳當下時間，與 Go 一致。
- E07-D014 `ctx.rand()`：每 session 一個 `random.Random`，以 init frame 的 seed（u64）建立，首次呼叫時建立，之後回傳同一物件；OnInit 之前呼叫拋錯。
- E07-D015 補 `.pyi`、`docs/python` 說明與一個使用 `now`/`rand` 的 example；文件 API 檢查腳本通過；測試確認 init frame seed 與 `rand()` seed 一致。同一 PR 版本升 1.3.0，不打 tag（tag 與 PyPI 由 Yi Te）。

#473（E07-05）把 hook 開始時間與 session seed 從 Rust 交到 Python。`Context::now_ns` 是這個 hook 的開始：dispatcher 跑的 hook 讀 `Dispatcher::hook_start_ns`（log 開啟時是輸入的 `t_ns`，關閉時是 begin_hook 時注入的時鐘，不再讀一次，E07-D009）；OnInit、OnStart，以及 Run 之前的 OnStop，由 plane 的 `outside` 在 hook 前讀一次 session clock，同一值寫進輸入的 `t_ns`。沒有 dispatcher 時（binding 測試的 `in_one_hook`）在進 hook 時讀一次。同一 hook 內每次呼叫都是這個值。Python 的 `Context.now` 在這個執行緒有 hook 在跑時回傳那個 hook 的開始，不看手上這個 token 是不是這個 hook 的；沒有 hook 時讀 session clock 的現在時間，不拋 `ContextError`（E07-D013）。

`Context::seed` 是 session 的 seed，Init 之前是 `None`。Python 的 `Context.rand` 檢查 token（過期仍是 `ContextError`），seed 還沒有時拋 `mftik.Error`，訊息 `sdk: the session has no seed before OnInit`。第一個成功的呼叫用這個 u64 建一個 `random.Random`，之後回傳同一個物件（E07-D014）。一個 process 一個 session。

`.pyi` 寫 `now() -> int` 與 `rand() -> random.Random`。`docs/python` 說明時間、亂數、執行緒與錯誤。`examples/python/sampler` 在每個 hook 打 `now` 與一次 `getrandbits(64)` 抽樣。文件 API 檢查腳本通過。版本升 1.3.0，不打 tag（E07-D006、E07-D015）。

與 Go 不同的地方。Go 的 `Now()` 是 step clock：執行中 hook 的開始，沒有 hook 時仍是上一個 hook 的開始。mftik 在 hook 外回傳當下時間（E07-D013）。Go 的 `Rand()` 是 `rand.New(rand.NewPCG(seed, 0))`；Python 是 `random.Random(seed)`（E07-D004）。關閉 log 時 hook 開始用注入的時鐘，不多讀一次（E07-D009）；Go 每次 beginHook 都讀牆鐘。

測試：`mftik-core` 的 `src/dispatch/tests.rs` 測 log 關時 hook 開始是注入的時鐘且不移動、log 開時是輸入的 `t_ns` 且不讀時鐘。`mftik-rt` 的 `src/plane.rs` 測 OnInit／OnStart／OnStop 與 dispatcher hook 的開始，以及 Init 前 seed 是 `None`；`tests/eventlog_session.rs` 每個 hook 印兩次 `now_ns` 與 seed，對上輸入的 `t_ns` 與 `init` 的 `ref`。mftik-py 的 in-process 測試測同一 hook 內 `now` 不變、過期 context 的 `now` 是正在跑的 hook 的開始、hook 外會走、`rand` 在沒有 seed 時拋錯；`tests/session.rs` 以安裝的 wheel、開著 event log，確認每個 hook 的 `now` 是輸入的 `t_ns`，`rand().getrandbits(64)` 的序列與 `random.Random(init.ref)` 相同（python CI job）。

## 決定（Yi Te，2026-11-01，E07-D016–E07-D019）

- **E07-D016** 拆票：#474 只做 Rust 回放引擎與公開 Rust API `mftik_rt::replay`，以純 Rust conform 策略測試；Python `mftik.replay` 包裝與文件移到 E07-07 #485。
- **E07-D017** 引擎沿用現有 frame reader 與 dispatcher，Now、Rand、Resolve、td/md 回覆、timer、Post 改為依紀錄注入，不連 NATS；行為對齊 Go PROTOCOL §12.6（比對項目、不重現的情況、cloid 自 init 計數器取號）。
- **E07-D018** `ReplayReport` 欄位同 Go：`mismatches`、`inputs`、`skipped`、`missing`、`orders`、`reads`；每筆 mismatch 記 rec_seq、種類、紀錄值與回放值。hook 結束方式在 Rust 比 panic 訊息；Python 比例外類別與訊息（E07-07）。
- **E07-D019** 驗收：錄製後回放零 mismatch；修改過的策略產生 mismatch；有缺號的紀錄正確計入 `missing`。phase 1 不比對 log（E07-D005）。
