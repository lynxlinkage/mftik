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
