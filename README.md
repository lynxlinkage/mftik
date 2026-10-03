# MFTIK

MFTIK 是一個自架的交易節點：策略用 Python 寫，節點負責把各平面的進程撐住、記下發生過的事，並提供操作介面。

這份 README 是這個 repo 的開發指引。使用者面的說明（SDK 怎麼用、`mftik` CLI、`mftik node-init` 起一個節點）在發佈到 PyPI 的 [`packages/common/README.md`](packages/common/README.md)。目前正在進行平面進程化重構，目標架構以 [`docs/ARCHITECTURE_CHANGE_PLAN.md`](docs/ARCHITECTURE_CHANGE_PLAN.md) 為準。

## 目錄導覽

| 路徑 | 內容 |
|---|---|
| `apps/api` | 控制平面。FastAPI，進程名 `mftik-api`；`routes/` 下的 REST 加 `/ws/*` 的 WebSocket |
| `apps/sts` | 策略平面，進程名 `sts`。session、hook 派送、OMS、event log |
| `apps/td` | 交易平面，進程名 `td`。下單、撤單、ledger |
| `apps/md` | 市場資料平面，進程名 `md`。公開 feed 扇出，tape 寫進同區的 Redis（`tape_store.py`） |
| `apps/sym` | symbol 平面，進程名 `sym`。tick、step、min notional |
| `apps/paper` | 同一個 stack 裡的模擬交易所，進程名 `paper` |
| `packages/common` | `import mftik`。protocol、broker、exchange adapter、strategy SDK、`mftik` CLI；也是 PyPI 上的 `mftik` 套件 |
| `packages/db` | `import mftik_db`。schema 與 Alembic migration |
| `contracts/openapi.json` | Python 與前端之間的 OpenAPI 契約，由 `just openapi` 產生 |
| `frontend/` | SvelteKit UI。不在 uv workspace 裡，用 npm |
| `deployment/` | 生產環境的宣告：Strategon 的 `sets/*.json`、`redis/redis.conf`、API 層的 `docker-compose.yml` |
| `scripts/` | 維運與量測腳本，多數有對應的 `just` recipe；`git-hooks/` 是 `install-hooks` 指過去的目錄 |
| `docs/` | 見下面的文件索引 |
| `Dockerfile` | 所有 Python 服務共用的單一 image，彼此只差 compose 裡的 `command` |
| `docker-compose.yml` | 本機開發 stack：postgres、redis、nats、六個平面、frontend |
| `docker-compose.peer.yml` | 疊在上面那份之上，在同一台機器起第二個節點（獨立 project name 與 host port），用來測節點之間的流程 |
| `conftest.py` | 整個 workspace 的 pytest 設定：event loop、sqlite/Postgres 參數化、啟動前先確認 broker 在 |

Apps 之間不互相 import。共用代碼只有 `packages/common` 和 `packages/db`。

## Quick start

需要 Docker、Python 3.12+、[`uv`](https://docs.astral.sh/uv/)、[`just`](https://just.systems/)，以及 Node（前端用 npm 裝）。

```bash
cp .env.example .env
just sync            # uv sync --all-packages + frontend npm install
just install-hooks   # pre-commit：OpenAPI 契約過期就擋在 commit
just up              # 先 build 共用 image，再 docker compose up
```

- API：<http://localhost:8000/health>
- UI：<http://localhost:5173>

用 `just up`，不要用 `docker compose up --build`。每個 Python 服務共用同一個 image tag，一次 build 全部會有好幾個 build 搶著寫同一個 tag。

測試需要 broker，而且沒有 fake 可以退回去——沒有 NATS，`conftest.py` 的 `pytest_sessionstart` 會直接讓整個 run 失敗：

```bash
just up -d nats      # 或者整個 stack 都起來
just test            # pytest，sqlite
just lint            # ruff
```

`just test` 只跑 sqlite。CI 另外在 Postgres 上跑一次（本機是 `just test-pg`，需要 `just up -d postgres`），因為 sqlite 忽略 VARCHAR 長度、也沒有 decimal 型別，欄位開太小在 sqlite 上看不出來。

其餘 recipe 用 `just --list` 看。常用的是 `just migrate`、`just seed`、`just openapi`、`just check-contracts`、`just frontend-check`、`just frontend-e2e`。

## 文件索引

重構期間以這三份為準：

| 文件 | 內容 |
|---|---|
| [`docs/ARCHITECTURE_CHANGE_PLAN.md`](docs/ARCHITECTURE_CHANGE_PLAN.md) | 平面進程化重構的計畫與決策（F1–F38）。基準是 `main` @ `a0cbfb2` |
| [`docs/REFACTOR_TICKETS.md`](docs/REFACTOR_TICKETS.md) | 上面那份計畫拆出來的工作票，每張對應一個 issue |
| [`docs/Deployment.md`](docs/Deployment.md) | 部署：Strategon plane sets、site 與 NATS gateway、secret、上線與回滾。B1-02（#161）會依現況重寫 |

重構的盤點結果在 `docs/baseline/`：[`closed-branches.md`](docs/baseline/closed-branches.md) 記下重構開始前被刪掉的分支的 head，[`protocol.md`](docs/baseline/protocol.md) 是現行協定的盤點（B0-03，#156），[`state-authority.md`](docs/baseline/state-authority.md) 是現況的狀態權威表（B0-04，#157）。

下面是舊模型的設計紀錄，都是英文寫的。B1-01（#160）會把它們整批移到 `docs/archive/`，並加一份 `INDEX.md` 記錄封存日期和取代它的文件；在那之前它們還在 `docs/` 根目錄。**內容描述的是重構前的架構，和計畫衝突時以計畫為準。**

| 分類 | 文件 |
|---|---|
| broker 與傳輸 | [`Broker.md`](docs/archive/Broker.md)、[`BrokerPatterns.md`](docs/archive/BrokerPatterns.md)、[`BrokerProvisioning.md`](docs/archive/BrokerProvisioning.md)、[`JetStreamRemoval.md`](docs/archive/JetStreamRemoval.md)、[`RedisRemoval.md`](docs/archive/RedisRemoval.md)、[`EventLoop.md`](docs/archive/EventLoop.md) |
| 平面與 instance | [`Instances.md`](docs/archive/Instances.md)、[`StsPause.md`](docs/archive/StsPause.md)、[`StsSessionList.md`](docs/archive/StsSessionList.md)、[`MdHandover.md`](docs/archive/MdHandover.md)、[`MdExpiry.md`](docs/archive/MdExpiry.md)、[`MdVenueSubscriptions.md`](docs/archive/MdVenueSubscriptions.md)、[`MdOpenInterest.md`](docs/archive/MdOpenInterest.md) |
| 功能設計 | [`Auth.md`](docs/archive/Auth.md)、[`AuditIdentity.md`](docs/archive/AuditIdentity.md)、[`Alert.md`](docs/archive/Alert.md)、[`Artifact.md`](docs/archive/Artifact.md)、[`StrategyEnvironment.md`](docs/archive/StrategyEnvironment.md)、[`CLI.md`](docs/archive/CLI.md) |
| venue 實測 | [`Deribit.md`](docs/archive/Deribit.md)、[`BitgetUta.md`](docs/archive/BitgetUta.md) |

已經封存的：[`docs/archive/README.md`](docs/archive/README.md) 是公開改寫前的 repo README，留著是為了舊的路徑與 recipe 對照表不要失傳。`docs/readme/` 是截圖，留給 B10 的 README 定稿用。

還沒有的：`docs/ARCHITECTURE.md`（B1-04，#163）寫定案後的目標架構，`docs/TESTING.md`（B2）寫測試標準。

## License

MIT。PyPI 上的 `mftik` 套件和這個 repo 同一個授權。
