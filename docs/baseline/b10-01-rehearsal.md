# B10-01 正式資料快照演練

這份步驟由 Yi Te 在**拋棄的** Postgres 16 還原庫上自己跑。不要對正式庫執行，不要對 compose 的 postgres 執行。

stdout 和 PR 留言只留 pass/fail、耗時、列數。不要貼 dump、列值、主鍵、URL、主機名稱，也不要附上快照檔。快照檔留在本機。

`<scratch-url>` 是同步 URL，驅動程式用 `postgresql+psycopg`。`<db>` 必須等於 URL 裡的資料庫名稱。腳本拒絕從 `DATABASE_URL`、`DATABASE_URL_SYNC` 或 `.env` 讀位址。

## 部署順序

下面第 6 步是在拋棄的還原庫上升級。正式環境要換到 `0037_drop_rebuild_facts` 時，順序不同：API、STS、TD、MD **全部先**換成含本 PR ORM 的 build，**然後**才把資料庫升到 0037。

`rebuild_count` 與 `st_facts` 是這張 PR 才從 ORM 拿掉的。`0036_session_code_identity` 以及更早的 build 仍會 SELECT 這兩欄，不是只有 0036 之前。`refactor/process-planes` 的 `83ae7e2` 模型裡還有這兩欄。欄位刪掉之後，那些 build 對 `sts_sessions` 的讀寫會全部失敗。

正式環境的 compose 在 API 主機上一次性跑 migrate（`scripts/deploy_prod_compose.sh` 的 `--profile tools run --rm migrate`，也就是 `alembic upgrade head`）。STS、TD、MD 是 plane，不在這個 compose 裡，要另外套用，而且各 plane 在其他主機上。那一次 migrate 不會換成那些 plane process。站台表裡 jp 的 plane 與 compose 宣告在同一台機器、tw 的 plane 在另一台；不論是否同一台，compose 都不會更新 plane。所以不能把部署 API 當成四個程序都換完：腳本會先升級資料庫，舊 ORM 的 plane 立刻讀寫不了 `sts_sessions`。

順序：含本 PR ORM 的 build 先套上 API、STS、TD、MD，最後才讓 API 主機上的 migrate 升到 `0037_drop_rebuild_facts`。正式切換步驟是 B10-04。

## 1. 事前

- 一台不是正式環境的 Postgres 16。
- 一份 Yi Te 自己取得的正式庫 dump。
- 這張 PR head 的 checkout。後續的 `mftik-db-migrate`、`alembic`、`scripts/b10_01_rehearse.py` 都用這個 checkout，不要用 `main` 的 image。

`mftik-db-migrate` 只會 `upgrade`，而且經由 `env.py` 讀 `DATABASE_URL_SYNC`。降版不能走它。

## 2. 還原到空的演練庫

```sh
createdb -h <host> -p <port> -U <user> <db>
pg_restore --no-owner --no-acl -d <scratch-url-for-pg_restore> <dump>
```

`pg_restore` 的連線字串用它自己的格式，不要把密碼寫進 repo。還原進空庫；不要還原進已經有別的 schema 的庫。

## 3. 升級前檢查

連上 `<scratch-url>`。`alembic_version` 必須**正好**是 `0034_strategy_type_key`。其他任何值都停，尤其是 `0035_sts_abort_target`（已關閉、從未合併的 PR #153）。那個版本號不在這條鏈上，`upgrade` 會以 `Can't locate revision` 失敗，而 `schema._ordinal` 會把它讀成 35。

```sql
SELECT version_num FROM alembic_version;
```

`sts_sessions` 不得有 `abort_target` 欄。有的話停。

```sql
SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name = 'sts_sessions'
   AND column_name = 'abort_target';
```

預期：零列。

記下列數、狀態分布，以及即將被丟掉的兩欄有多少不是預設值。`st_facts` 是 `json`，用文字比較。

```sql
SELECT status, restart, count(*)
  FROM sts_sessions
 GROUP BY status, restart
 ORDER BY status, restart;

SELECT status, count(*) FROM td_sessions GROUP BY status ORDER BY status;
SELECT status, count(*) FROM md_sessions GROUP BY status ORDER BY status;

SELECT count(*) FILTER (WHERE st_facts::text <> '{}') AS nonempty_st_facts,
       count(*) FILTER (WHERE rebuild_count > 0)     AS rebuild_count_gt_0
  FROM sts_sessions;
```

各表精確列數以第 5 步腳本印出的 `table=` 行為準。

## 4. 匯出即將丟掉的兩欄

降版只會把欄加回來，值回不來（每一列都是 `rebuild_count = 0`、`st_facts = {}`）。先把副本留在本機。

```sql
\copy (SELECT session_id, rebuild_count, st_facts
         FROM sts_sessions
        ORDER BY session_id)
  TO '<export.csv>' WITH (FORMAT csv, HEADER true)
```

`<export.csv>` 不要提交，不要貼進 PR。

## 5. 快照

在 PR checkout：

```sh
uv run --all-packages python scripts/b10_01_rehearse.py snapshot \
  --url '<scratch-url>' \
  --database '<db>' \
  --out '<snapshot.json>'
```

預期第一行 `pass`，`revision=0034_strategy_type_key`，接著 `tables=`、`rows=`，以及每個 `table=<name> rows=<n>`。`<snapshot.json>` 只有列數、欄名、digest，沒有儲存格、沒有主鍵。留在本機。

不是 `0034_strategy_type_key` 就停，不要升級。

## 6. 計時升級

```sh
DATABASE_URL_SYNC='<scratch-url>' /usr/bin/time -p \
  uv run --all-packages mftik-db-migrate head
```

`mftik-db-migrate` 只接受目標 revision，預設 `head`。它不會降版。

預期：指令結束碼 0，`alembic_version` 變成 `0037_drop_rebuild_facts`。記下 `real` 秒數。

## 7. `alembic current` 與 `alembic check`

```sh
DATABASE_URL_SYNC='<scratch-url>' \
  uv run --all-packages alembic -c packages/db/alembic.ini current
DATABASE_URL_SYNC='<scratch-url>' \
  uv run --all-packages alembic -c packages/db/alembic.ini check
```

預期：`current` 印 `0037_drop_rebuild_facts (head)`。`check` 沒有 diff，結束碼 0。有 diff 就是失敗，不要繼續當通過。

## 8. 升級後驗證

```sh
uv run --all-packages python scripts/b10_01_rehearse.py verify \
  --url '<scratch-url>' \
  --database '<db>' \
  --snapshot '<snapshot.json>'
```

在 `0037_drop_rebuild_facts` 上，verify 檢查：存活欄的 digest 與列數不變、`rebuild_count` 與 `st_facts` 已不在、`alembic check` 乾淨、`sts_sessions` / `td_sessions` / `md_sessions` 都能走現在的 repository 讀取。

它也會確認序號還接得下去：對 `users`、`td_sessions`、`md_sessions` 各插入一列再刪掉，其餘 serial 各呼叫一次 `nextval`。Postgres 的 `nextval` 不跟著交易回滾，所以演練庫的序號會往前一號。這是拋棄的還原庫。不要對正式庫跑。

預期：第一行 `pass`，`revision=0037_drop_rebuild_facts`，`rows=` 與第 5 步相同。`fail` 或 `error=withheld` 都是失敗。`error=withheld` 表示例外文字可能含列值，所以腳本不印；不要為了貼 PR 而把 traceback 貼出來。

## 9. 降版演練

必須用**這個** checkout。`main` image 的 migrate 不認識 `0035` 以後的 revision，無法降回來。

```sh
DATABASE_URL_SYNC='<scratch-url>' \
  uv run --all-packages alembic -c packages/db/alembic.ini \
  downgrade 0034_strategy_type_key
```

預期：`alembic_version` 回到 `0034_strategy_type_key`。`0035` 的 downgrade 會把 `restart` 縮回 `String(8)`。`main` 上的值只有 `always` / `never`，演練降版會過。一旦有列是 `on_failure`（10 個字元），這一步會失敗。那是 B10-04 的回滾限制，這張票不改 `0035`。

然後對同一份快照再 verify。此時 revision 是 `0034`，檢查改為：表與欄回到快照當時的形狀、digest 仍相符、兩欄讀回來是伺服器預設（`rebuild_count` 全部是 0，`st_facts` 全部是 `{}`）。資料沒有跟著欄一起回來。

```sh
uv run --all-packages python scripts/b10_01_rehearse.py verify \
  --url '<scratch-url>' \
  --database '<db>' \
  --snapshot '<snapshot.json>'
```

預期：`pass`，`revision=0034_strategy_type_key`。

若要把第 4 步的副本裝回去（選擇性，只在降版之後、欄已經存在時）：

```sql
CREATE TEMP TABLE sts_facts_export (
    session_id text PRIMARY KEY,
    rebuild_count integer,
    st_facts json
);
\copy sts_facts_export FROM '<export.csv>' WITH (FORMAT csv, HEADER true)
UPDATE sts_sessions AS s
   SET rebuild_count = e.rebuild_count,
       st_facts = e.st_facts
  FROM sts_facts_export AS e
 WHERE s.session_id = e.session_id;
```

沒有這一步，`main` 的 ORM 讀得到表，但兩欄是預設值，不是升級前的內容。

### 部署腳本的回滾不是資料庫回滾

`scripts/deploy_prod_compose.sh` 的 `rollback` 只把 compose 備份放回去、把 `MFTIK_VERSION` 改回舊 tag，然後 `docker compose up -d`。它不執行 `alembic downgrade`。

這支 revision 跑過之後，`main` 的 image 選不到已經不存在的 `rebuild_count` 與 `st_facts`，在資料庫降回 `0034_strategy_type_key` 之前無法讀 `sts_sessions`。image tag 退回不夠。正式切換要先把 API、STS、TD、MD 換成含本 PR ORM 的 build，再升資料庫，見上面「部署順序」。回滾手順是 B10-04，不在這張票。

## 10. 再升級

```sh
DATABASE_URL_SYNC='<scratch-url>' \
  uv run --all-packages mftik-db-migrate head
uv run --all-packages python scripts/b10_01_rehearse.py verify \
  --url '<scratch-url>' \
  --database '<db>' \
  --snapshot '<snapshot.json>'
```

預期：`current` 又是 `0037_drop_rebuild_facts`，verify 第一行 `pass`。若第 9 步把 CSV 裝回去了，這次升級會再丟掉那兩欄；存活欄的 digest 仍應與快照一致。

## 11. 什麼算通過，以及 PR 留言貼什麼

通過：第 3 步 revision 正好是 `0034_strategy_type_key` 且沒有 `abort_target`；第 6 步結束碼 0；第 7 步 `check` 乾淨；第 8、9、10 步 verify 都是 `pass`，而且 `rows=` 三次相同。

失敗：revision 不是上面兩個之一、`check` 有 diff、verify 印 `fail` 或 `error=withheld`、降版因 `restart` 寬度失敗、列數或 digest 對不上。

PR 留言只貼這些（數字換成你看到的，不要加列值）：

```
b10-01 rehearsal: pass
revision before: 0034_strategy_type_key
abort_target rows: 0
nonempty st_facts: <n>
rebuild_count > 0: <n>
sts status/restart counts: <status> <restart> <n> ...
td status counts: <status> <n> ...
md status counts: <status> <n> ...
snapshot rows: <n>
migrate real_s: <s>
alembic current: 0037_drop_rebuild_facts
alembic check: clean
verify head: pass elapsed_s=<s> rows=<n>
downgrade verify: pass elapsed_s=<s> rows=<n>
verify head again: pass elapsed_s=<s> rows=<n>
```

失敗時改成 `b10-01 rehearsal: fail`，加上失敗的步驟編號與腳本印出的 `problem=` 行（那些行只有表名和計數）。不要貼 traceback。
