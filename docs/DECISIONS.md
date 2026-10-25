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

## 決定（2026-10-25，E03-D041–E03-D045）

E03-04 SYM client。語意對齊 mftiko PROTOCOL §7.3、§9.4 第 8 條（E08-D004、E08-D022–E08-D026、E04-D057）與 `sdk/internal/symclient`。`symclient` 與 `symbol` 放 `mftik-core`，NATS、計時器、resolve 的阻塞等待留給 host（E03-D001）。

- E03-D041 介面：`symclient::Client` 吃事件與該次呼叫的 `now_ns`，吐 `Request`、`Subscribe`、`Unsubscribe`、`ArmTimer`、`Stored`、`Resolved`、`Log`。`Request` 帶 `Ticket`、`sym.v1.Request`、`wait` 與 `resolve`。`wait` 是 5 秒，Go 的預設；`resolve` 有值表示那個 resolve 正卡在這個請求上，hook 有期限時 host 改等到期限，與 Go 用 hook 的 ctx 相同。回覆以 `on_reply(ticket, Outcome)` 交回：`Body` 是收到的原始 bytes，natsx error reply（0x00 開頭）在 core 解，code 2 且訊息是 `not_found` 或以 `not_found:` 開頭才算查不到；`Failed` 是逾時、沒有 responder 或送不出去，文字由 host 給。`on_subscribed`、`on_changed(subject, bytes)`、`on_reconnect`、`on_timer` 同一個模式。`info(uid)` 只讀記憶體，不產生 effect。`close` 讓在途的 resolve 以 `Closed` 結束、每個發過 `Subscribe` 的 subject 都 `Unsubscribe`，之後的回覆、通知、計時器都忽略，`info` 仍可讀。
- E03-D042 resolve：host 只在 `OnInit`、`OnStart` 期間 `allow_resolve(true)`。窗外回 `NotInInit`，ticker 不合 strict parse 回 `InvalidTicker`，close 之後回 `Closed`，三者都同步、不送任何東西。之後照 Go 的順序：`GetRequest`（不設 `include_inactive`）；該表第一次時 `Subscribe`，等 `on_subscribed`，失敗則 resolve 失敗、列不存，下一次 resolve 再訂；存列並發 `Stored`；`Revisions` 與已確認的 revision 比，相同就結束，否則跑一輪重拿；最後 `Resolved` 帶當時的列。比對或重拿失敗只記 log，resolve 仍成功。SYM 查不到是 `NotFound`，不追蹤；策略回傳它就以 `INIT_FAILED` 結束。
- E03-D043 重拿：單位是 `(venue, category)`，同一張表同時只有一個比對或重拿。一輪是 `Revisions`、依 UID 順序對每個追蹤的列 `GetByUidRequest`、再 `Revisions`。兩次相同、而且這輪中間沒有 resolve 加進這輪沒列到的列，才確認 revision；否則重跑，最多 8 輪。活躍表 `not_found` 改以 `include_inactive` 再問；仍失敗，或回來的 UID、表不對，這輪失敗，已存的列保留，確認的 revision 不動。通知的 revision 大於確認值才重拿；解不開、subject 不符、revision 小於 1，改做全表比對（Go 的 `Reconnect`）。重拿進行中收到的通知記成 `want`，比對發現落後記成 `again`，結束後若仍需要再跑一輪。全表比對同時只有一個，期間再要一次就在結束後再跑一次。與 Go 不同處：Go 以 `loadMu` 把 resolve 與重拿排成一條；這裡 resolve 的 `GetRequest` 立刻送出，不排在背景重拿之後，不同表的重拿可以並行，以「新列使這輪不能確認」保住同一個不變式。
- E03-D044 計時與 log：約 5 分鐘的比對是 `ArmTimer`，第一筆列存入時才 arm，每次觸發後從該次 `now_ns` 再 arm（Go 是從 `Open` 起的固定 ticker）。只認最後一個 token。沒有追蹤任何表時，計時器、重連、壞通知都不送請求。log 沿用 Go 的文字：`symclient: sync {venue} {category}: …`、`symclient: reload {venue} {category}: …`、`symclient: revision check: …`；比對時每張失敗的表各記一行，不像 Go 併成一則。
- E03-D045 `Info` 與 bookpath：`symbol::Info` 對應 `contract/symbol.Info`，欄位一對一。proto 沒設的 optional 數字是 0、字串是空；filter 是 `BTreeMap<String, f64>`，有名字沒有界限的是 NaN（E00-D008）。不另加不活躍欄位，終結的 `Status` 就是標記（E08-D026）。`price_tick()` 回 `f64`：沒有這個 filter 是 0，沒有界限是 NaN，與 Go 的 `Filters[PriceTick]` 相同，bookpath 兩者都拒絕。每個 `Stored{uid}`，host 以 `bookpath::Path::resolve(uid, info.price_tick())` 更新 tick（E02-D023）。`symbol` 只移植 SDK 用到的：strict parse（含 strike 正規化檢查）、`Category`、`Status`、`valid_uid`、filter 名稱、margin 分組；寬鬆的 `Resolve`、`Listing`、資產別名表、`ExchKey` 不移植。大寫逐字元轉換：Go 用 simple case mapping，std 只有 full mapping，大寫是多個字元的字（如 `ß`）保持不變，與 Go 相同；少數帶 ypogegrammeni 的希臘字母與 Go 不同。
