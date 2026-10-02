# 部署

mftik 現在實際上怎麼部署。依據是 repo 裡的檔案（`deployment/sets/*.json`、
`deployment/redis/redis.conf`、`deployment/docker-compose.yml`、`scripts/s7n.py`、
`scripts/deploy_prod_compose.sh`、`.github/workflows/release.yml`、`justfile`），
加上 2026-10-01 在 `cp`（site jp）上做的一次唯讀巡檢。

兩個慣例：

- 每一節開頭標出**對應檔案**。沒有對應檔案的，標成**只在主機上**——意思是它不在
  repo 裡、也不在 CI 裡，只存在那台機器或操作者自己的機器上。
- `yite`（site tw）這次沒有巡檢。凡是只有 tw 才成立的敘述，都標成**未驗證**：
  repo 只能證明「宣告過」，證明不了現在跑著什麼、跑的是哪一版。

## 三層，以及為什麼 tag 不碰 infra

| 層 | 內容 | 宣告在哪 | 誰滾動 |
|---|---|---|---|
| infra | NATS、每個 site 一台 Redis | `deployment/sets/infra.json` | 人工，很少 |
| planes | td、sts、md、sym、paper | `deployment/sets/planes.json` | final tag（`release.yml` 的 `planes` job） |
| API | api、frontend | `deployment/docker-compose.yml` | final tag（`api` job，走 `scripts/deploy_prod_compose.sh`） |

bus 和 tape disk 沒有理由因為策略改了而重啟。一個會滾動它們的 tag，等於為了一個
sizing 修正把所有 session 帶下來，而丟掉 tape 等於丟掉一段沒有 venue 補得回來的
warm-up 視窗。所以 release workflow 只套用 plane sets 和 API compose，從不碰 infra
（`.github/workflows/release.yml:9`–`13`）。

兩層都是 **AssignmentSet**，連只有一個成員的也是。set 才能讓 per-member 的身分
由控制平面展開，而不是每個進程複製一段設定（`scripts/s7n.py` 的 module docstring）。

## 站台與機器

**對應檔案：** `deployment/sets/*.json` 的 `machine` 與 `member.vars`。

| 機器 | site | 身分 |
|---|---|---|
| `cp` | jp | plane 成員 `td-jp`、`sts-jp`、`md-jp`；infra 成員 `nats-jp`、`redis-jp`；同時是 compose host `mftik.lynkora.com` |
| `yite` | tw | plane 成員 `td-tw`、`sts-tw`、`md-tw`、`sym-tw`、`paper-tw`；infra 成員 `nats-tw`、`redis-tw`（未驗證） |

Tailscale 位址只有兩個，寫在 set 的 `member.vars` 裡：jp 是 `100.108.10.2`，
tw 是 `100.65.26.119`。平面之間、平面到 NATS 都走這兩個位址。

tw 存在的理由是合規，不是容量：有些 venue 不接受日本 IP，而一組憑證的管轄權不能
協商。`sym` 和 `paper` 是反方向的例外——兩者都不在熱路徑上，所以放在記憶體有餘裕
的那台。

**只在主機上**（2026-10-01 在 `cp` 上確認）：`cp` 是 Ubuntu 24.04.3、kernel 6.8，
除了上面那些 assignment，還跑著 Traefik、API、frontend、SeaweedFS（artifact 的物件
儲存）和 Strategon 控制平面本身的 docker 容器。要往 JP 加東西之前先看這台。

舊版文件記錄的硬體與延遲數字（`cp` 2 核 4 GB、`yite` 16 核 33 GB、venue RTT
3–4 ms 對 113–147 ms、`cp`↔`yite` 35 ms）是先前的人工量測，**本次沒有重測**。

## plane sets

**對應檔案：** `deployment/sets/planes.json`。

| set | `strategy` | 成員（機器） |
|---|---|---|
| `td` | `td` | `td-jp`（cp）、`td-tw`（yite，未驗證） |
| `sts` | `sts` | `sts-jp`（cp）、`sts-tw`（yite，未驗證） |
| `md` | `md` | `md-jp`（cp）、`md-tw`（yite，未驗證） |
| `sym` | `sym` | `sym-tw`（yite，未驗證） |
| `paper` | `paper` | `paper-tw`（yite，未驗證） |

成員名字同時是 assignment slot、WorkDir 的路徑片段、`MFTIK_INSTANCE`，以及
dashboard 和 STS 放置會讀的 `instances.name`。`instances.name` 不可改名
（`packages/db/src/mftik_db/models/instance.py:29`–`36`）：進程是從
`MFTIK_INSTANCE` 認識自己的，API 寫不到那個環境。`region` 不是標籤——沒有指名
instance 的 STS session，會被放到「該憑證的 TD 所在 region 裡唯一啟用的 STS」
（同檔 `:53`–`60`）。`sym` 和 `paper` 的 template 不設 `MFTIK_INSTANCE`，也沒有
`instances` 列。

**一個角色一個 set**，不是一個八成員的大 set：`template.env` 是同一個 set 的成員共用
的，只有值能透過 `${member.vars.X}` 變。一個大 set 就得把 `REDIS_URL` 一起發給 STS，
也得把 registry volume 掛進每個平面。拆開的附帶好處是 TD 的滾動不會連動 MD，而
`update.maxUnavailable: 1`（`planes.json:24`）讓同一個角色的兩個 region 不會同時下線。
控制平面是用成員名字還是 family 當 assignment key，`just s7n-status` 的 `key=` 欄位
會印出來（`scripts/s7n.py:563`）；本次沒有在主機上核對這個欄位。

### env 從四個地方來

`cp` 上三個平面實際的環境變數，正好等於下面四者相加：

| 來源 | 內容 |
|---|---|
| `planes.json` 的 `commonEnv`（`:9`–`15`） | `BROKER_KEY_PREFIX`、`LOG_LEVEL`、`MFTIK_DEFAULT_USER_ID`，以及兩個 `secret.*` token |
| 各 set 的 `template.env` | `MFTIK_INSTANCE`、`NATS_URL`，以及角色自己的設定（STS 的 `MFTIK_DATA`、`STS_*`；MD 的 `REDIS_URL`、`MD_TAPE_*`；SYM 的 `SYM_REFRESH_INTERVAL`） |
| 控制平面 | 把 `secret.mftik-database-url` / `-sync` 換成 `DATABASE_URL` / `DATABASE_URL_SYNC` 的明文（見「secret 在哪」） |
| image 本身 | `ALEMBIC_CONFIG`（`Dockerfile:24`）、`MFTIK_DIST_VERSION`（`Dockerfile:37`–`38`，由 release workflow 以 build-arg 帶入） |

`render_sets` 會把檔案層級的 `commonEnv`、`deployPolicy`、`limits`、`captureStdio`、
`update` 折進每個 set，set 自己寫的值優先（`scripts/s7n.py:273`、`:296`–`300`）。

template 裡只用三種展開：`${member.name}`、`${member.vars.*}`，以及 infra 的
`${CONFIG}`。其他路徑都是絕對路徑或 volume mount。

### volume

`planes.json:5`–`8` 宣告兩個機器層級的 volume（`cp` 和 `yite` 各一個
`mftik-data`），`apply` 在動任何 set 之前把缺的建起來（`scripts/s7n.py:390`）。
只有 STS 掛它（`:50`–`52`，掛在 `/var/lib/mftik`），registry、artifacts 和 session
event log 都在裡面（`:56`–`59`）。其他 set 什麼都不掛：tape 是該 site 的 Redis，
ledger 在 TD 的記憶體裡，SYM 和 paper 不存東西。

mount 必須指名一個已宣告的 volume，否則 preflight 直接拒絕
（`scripts/s7n.py:378`–`384`）。照 template 要什麼就建什麼，會把一個拼錯的名字變成
一個開在空 registry 上的 STS，而 registry 是節點唯一重建不出來的目錄。

API 的 push、delete 和 remote connect 會把改動的 tree 以 `sts.registry.sync` 送到
每個 STS（`packages/common/src/mftik/protocol/messages.py:1601`、
`apps/sts/src/mftik_sts/rpc/registry.py:221`），所以不共用 API volume 的主機也收得到。

STS 開機與 `sts.registry.sync` 會把 `registry/{public,private,pulled/<remote>}/<name>/`
複製進 `registry/trees/<digest>/`，然後把舊目錄留在原地。compose 的 `api` 和 `sts`
共用 `mftik_data`（`/var/lib/mftik`）時，那些目錄就是 API 的 `RegistryStore`；digest
仍是 null 的 session 重新掛起時也還是從它們載入。這一步不刪目錄。清掉舊布局是 B10
切換的事，由 API 搬自己的 store 時一起處理。

### deployPolicy

`planes.json:16`–`22`：`startsecs: 5`、`healthWindowSeconds: 90`、
`maxCrashesInWindow: 3`、`stopGraceSeconds: 15`、`enableAutoRollback: true`。
plane set 沒有宣告 `readiness`，所以健康與否就是「起得來、撐過 `startsecs`、沒有在
視窗內 crash 超過三次」。

`limits.memoryBytes`（`:23`）**目前沒有生效**，見「記憶體上限目前沒有生效」。

## OCI 與 `captureStdio`

**對應檔案：** `deployment/sets/planes.json`、`Dockerfile`、
`.github/workflows/release.yml` 的 `planes` job；目錄佈局**只在主機上**。

平面跑在 Strategon 的 OCI driver 底下，而它不是 runc 也不是 containerd：agent 把自己
re-exec 成 `--oci-init`，用 user / mount / PID namespace 加 `setsid` 起進程，
`pivot_root` 進 rootfs，網路用 host（[`docs/ARCHITECTURE_CHANGE_PLAN.md`](ARCHITECTURE_CHANGE_PLAN.md)
§4.5，以下簡稱「計畫」）。網路是 host 的，所以 `md-jp` 用
`redis://127.0.0.1:6379/0` 就連得到同一台機器上只聽 loopback 的 Redis；PID、user 和
mount namespace 則是每個平面各自一套（2026-10-01 在 `cp` 上確認）。

rootfs 來自 `docker save` 的 tar：`planes` job 先 `docker pull --platform linux/amd64`
再 `docker save`，上傳成每個 set 的 artifact（`release.yml:341`–`357`），agent 解壓到
`releases/<version>/rootfs`。

**只在主機上**的佈局：
`/var/lib/strategon-agent/strategies/<member>/{current -> releases/<ver>, releases/, work/, .stdio/}`，
外加機器層級的 `volumes/mftik-data` 和 `shared/`。OCI driver 只 bind-mount work 目錄、
shared 目錄、config 檔和 template 的 `volumeMounts`；寫在這些之外的檔案落在
`releases/<version>/rootfs` 裡面，**會跟著那個 release 一起被 GC 刪掉**。這就是
`STS_EVENTLOG_DIR` 和 `STS_ARTIFACT_DIR` 指進 volume 而不是指進容器內某個路徑的原因
（`planes.json:58`–`59`）。

`captureStdio: true` 寫在檔案層級（`planes.json:25`），由 `render_sets` 折進五個
set，所以五個平面全開；`infra.json` 沒有宣告這個欄位。開著的時候 Strategon 的 tee
是新 PID namespace 的 PID 1，payload 是它的 child（計畫 §4.5），stdout / stderr 落在
該成員的 `.stdio/`。這也是 set 持有的 slot 唯一的 stdio 寫入路徑——per-strategy 的
`SetStdioCapture` 在這些 slot 上會被拒絕（計畫 §4.5，Strategon 端，本次未另外驗證）。

**平面滾動會殺掉它 spawn 出來的所有子進程。** 2026-10-01 在 `cp` 上確認：STS 的
session worker 和 STS 平面共用同一個 PID namespace。tee 是這個 namespace 的 init，
它一死，kernel 就 SIGKILL 整個 namespace。所以滾動 `sts` set 等於把該成員上所有
session 殺掉；現在靠 `STS_REBUILD_ON_BOOT=1`（`planes.json:57`、
`apps/sts/src/mftik_sts/app.py:137`–`139`）在新版本起來之後重建，巡檢當時 `cp` 上四個
session worker 都是 `python -m mftik_sts.worker <sid> rebuild`。計畫 §4.5 的 (A)
（`oci_host_pid`，strategon#60）就是為了改掉這件事。

## NATS

**對應檔案：** `deployment/sets/infra.json`；**`deployment/nats/nats.conf` 不在 repo 裡**。

每個 site 一台，中間是 **gateway**，不是 cluster：

```
jp   nats-jp  on cp     100.108.10.2:4222   gateway 100.108.10.2:7222
tw   nats-tw  on yite   100.65.26.119:4222  gateway 100.65.26.119:7222   （未驗證）
```

位址、advertise、gateway 名字和對側的 URL 全部來自 `infra.json:35`–`56` 的
`member.vars`。cluster 的 route 會 gossip 出全網格並假設資料中心級的延遲，而
`cp`↔`yite` 是幾十毫秒；gateway 傳播的是 interest，讓每個 site 的 client 留在自己的
server 上——JP 平面的 request-reply 在 JP 就被回答，除非唯一的 responder 在 TW。

config 裡**沒有 `jetstream` 區塊，也沒有 `leafnodes` 區塊**（2026-10-01 在 `cp` 上
讀到的 config 確認）。所有 store family 都離開 JetStream 了，所以一個 site 一台不是
退化的 cluster，而是完整的 site：沒有 meta group，就沒有 quorum 要維持。每個平面都
以普通 client 連上去。

版本：`infra.json:8` 的 `artifactVersion` 是 `2.14.6`，`cp` 上跑的就是
nats-server 2.14.6。**本機開發和 CI 用的是 `nats:2.11-alpine`**
（repo 根目錄的 `docker-compose.yml:64`、`release.yml:58`），和生產不同版。

readiness 是 `http://127.0.0.1:8222/healthz`（`infra.json:23`–`25`），和
`NATS_HTTP_LISTEN` 的 loopback 位址對應。`limits.memoryBytes` 宣告 512 MiB，同樣
沒有生效。

### `nats.conf` 在哪、怎麼產生、怎麼送上去

`deployment/.gitignore` 排除 `nats/`，理由寫在那個檔案裡：它帶帳號密碼。所以
`deployment/nats/nats.conf` **不在 repo、也不在 CI**，它有三份：

1. 操作者自己機器上的 checkout（**只在主機上**。要套用 infra 就必須有這一份）；
2. 控制平面 artifact catalog 裡的 `nats-config`，版本是 `infra.json:9` 的
   `configVersion`（目前 `v1`，和 `cp` 上跑的一致）；
3. 主機上 agent 展開的 `.../nats-jp/releases/<artifactVersion>/config`，由
   `current` 指過去（**只在主機上**）。

**怎麼產生：** 手寫，一份檔案給整個 fleet。站台之間的差異只有 env，由
`infra.json:12`–`22` 的 `template.env` 填：`NATS_SERVER_NAME`、
`NATS_CLIENT_LISTEN`、`NATS_CLIENT_ADVERTISE`、`NATS_HTTP_LISTEN`、
`NATS_GATEWAY_NAME`、`NATS_GATEWAY_LISTEN`、`NATS_GATEWAY_ADVERTISE`、
`NATS_REMOTE_GW`、`NATS_REMOTE_GW_URL`。這九個名字就是本機那份檔案要滿足的契約：
`server_name`、`listen`、`client_advertise`、`http` 和 gateway 的欄位都寫成
`$NATS_*`，由 nats-server 自己從環境展開。除此之外，config 裡有
`system_account: SYS`、`no_auth_user`，以及 `SYS` 和 `APP` 兩個 account 連同使用者和
密碼；**沒有** `write_deadline`、`max_pending`、`max_payload` 的覆寫，所以這三項是
server 預設（以上是 2026-10-01 在 `cp` 上讀到的內容，不含值）。

**怎麼送上去：** 跟著 infra 的 `apply` 一起上傳成 `nats-config` artifact
（`scripts/s7n.py:530`–`537`），agent 再抓下來放進 `releases/<ver>/config`，
template 的 args 是 `["-c", "${CONFIG}"]`（`infra.json:11`）。

**改內容一定要先把 `configVersion` 加一。** 同一個版本換 digest 會被直接拒絕
（`scripts/s7n.py:207`–`211`），而版本號也是 agent 判斷要不要重抓的依據。

## Redis

**對應檔案：** `deployment/sets/infra.json`、`deployment/redis/redis.conf`。

每個 site 一台，放在推該 site 行情的 MD 旁邊，只聽 loopback
（`redis.conf:13`）。STS 從來沒有 `REDIS_URL`——一個在 JP 行情上 warm-up 的 TW
session 會開到錯的 disk，不聽 Tailscale 就是在強制這件事。唯一的 caller 是同一台
機器上的 MD（`planes.json:80`）。

耐久性是 `appendonly yes`（`redis.conf:22`）加上一個撐得過版本滾動的目錄：
`dir ./` 是 agent 的 strategy 目錄，也就是 exec driver 的 cwd，它在 `releases/`
之外。`maxmemory 512mb` 配 `maxmemory-policy noeviction`（`:37`–`38`）：滿了就是拒絕
新的 print，這是 MD 本來就能容忍的（append 不能擋住 live 的 fan-out）；eviction 會
反過來悄悄縮短一段已經告訴策略覆蓋範圍的 warm-up 視窗。

binary 是 Ubuntu 的 `redis-server` 8.2.1（`infra.json:64`），`cp` 上跑的也是 8.2.1，
聽在 `127.0.0.1:6379`。這個 set 沒有宣告 `readiness`。

**`infra.json` 的 redis config 版本落後於現況。** `infra.json:65` 寫
`configVersion: "v1"`，但 jp 上跑的是 `redis-config` **v2**（2026-10-01 確認）。
照現在的檔案套用 infra，會把 Redis 的 config 退回 v1。要先把 `redis.conf` 和
`configVersion` 跟線上對齊，再套用 infra 層。

## 記憶體上限目前沒有生效

**對應檔案：** `deployment/sets/planes.json:23`、`deployment/sets/infra.json:33`、`:75`；
其餘**只在主機上**（2026-10-01，`cp`）。

宣告的上限是 plane 768 MiB（`805306368`）、NATS 512 MiB、Redis 1 GiB。實際上：

- 所有被監督的進程都在**同一個 cgroup** `0::/system.slice/strategon-agent.service`
  裡，它的 `memory.max` 是 `max`；
- 沒有任何 per-assignment 的子 cgroup；
- agent 的 unit 是 `MemoryMax=infinity`，ExecStart **沒有 `--cgroup-root`**。

所以三個 `limits.memoryBytes` 都沒有效果：五個平面、NATS、Redis，以及跑使用者策略
代碼的 STS session worker，共用整台主機的記憶體，彼此之間沒有上限，而且都以 agent
unit 的那一個非 root user 執行。巡檢當時的 RSS 大約是 sts 101 MB、td 108 MB、
md 116 MB、每個 session worker 102 MB、redis 25 MB、nats 21 MB。

計畫的對應段落是 §4.5（事實與推論）和 §4.7（F7：`oom_score_adj` 分級、可選的
`RLIMIT_DATA`、准入控制）。cgroup 上限本身追蹤於 strategon#61，不是這次重構的前提。

### 重啟 agent 會殺掉所有東西

同一個設定的另一個後果：agent 的 unit 是 `KillMode=control-group`，`Delegate=no`。
`systemctl restart strategon-agent` 會殺掉那個 cgroup 裡的所有進程——五個
assignment 全部，加上它們的子進程。

這在 `cp` 上實際發生過：2026-10-01 14:20（UTC+8）agent 重啟，新的 agent 對五個
assignment（`nats-jp`、`redis-jp`、`sts-jp`、`td-jp`、`md-jp`）都記下
`adopt skipped ... not running`，所有 session 以 rebuild 重來。**升級 agent 要當成
一次全站重啟來安排**，不能當成只換監督者。計畫 §4.5 的 S-3 要把 unit 改成
`KillMode=process`（strategon#60）。

## secret 在哪

**對應檔案：** `deployment/sets/planes.json:13`–`14`、`scripts/s7n.py`、
`.github/workflows/release.yml`、`justfile:136`–`142`；API 層的 `.env` **只在主機上**。

四個地方，沒有第五個：

**1. 平面的資料庫 URL：Strategon 的 secret catalog。** spec 裡只有 token：

```json
"DATABASE_URL": "secret.mftik-database-url",
"DATABASE_URL_SYNC": "secret.mftik-database-url-sync"
```

控制平面在寫成員的 assignment 時才換成明文，而且只往南送；讀回來只有長度和
key id（`scripts/s7n.py:596`–`601`）。所以 `planes.json` 自己就是完整的、可以貼出來
的，`just s7n-plan` 能印出一個 tag 會套用什麼，而 release workflow 只需要帶一個
secret：Strategon 的 token。

放進去用 `just s7n-secret-put <name>`（`justfile:141`），值從 stdin、`--from-env` 或
`--from-file` 來，**永遠不走 argv**（`scripts/s7n.py:577`–`591`）。`apply` 會先拿
rendered env 裡每一個 `secret.*` 去比對 `ListSecrets`，少一個就在動第一個 set 之前
失敗（`:370`–`376`）——另一種結局是成員起來、resolve 時 fail closed，然後在 set 回滾
的同時把那個 region 的 TD 帶下來。刪除 secret 不會改動引用它的 set，它們會在下一次
滾動時 fail closed（`:611`–`616`）。輪替＝同名 `put`，再 re-apply。

**2. CI：GitHub Actions secrets。** `release.yml` 只用到四個：`planes` job 的
`STRATEGON_API_KEY`（`:351`）、`api` job 的 `SSH_HOST` / `SSH_USER` /
`SSH_PRIVATE_KEY`（`:373`–`375`），和 `notify` 的 `DISCORD_WEBHOOK_URL`（`:392`）。
`planes` job 完全不碰資料庫憑證。

**3. API 層：compose 旁邊的 `.env`（只在主機上）。** 它是 root-only、0600，不在
repo、也不在 CI。部署只改寫其中 `MFTIK_VERSION` 那一行，並且在改之前把整個檔案備份
成 `.env.bak-<sha>`（`scripts/deploy_prod_compose.sh:72`–`76`）。

**4. NATS 的 account 密碼：在 `nats.conf` 裡**，見上面 NATS 那一節。

## 部署與回滾

### 平面：一個 tag

**對應檔案：** `.github/workflows/release.yml`。

1. `test` → `build`（每個架構各自一台 runner，push by digest）→ `merge`
   （拼成 manifest list；只有 final tag 會移動 `:latest`）。
2. `pypi`、`planes`、`api` 三個 job 都只在 final tag 上跑
   （`github.ref_type == 'tag' && !contains(github.ref_name, '-')`，`:274`、`:327`、
   `:364`）。prerelease（`v1.2.3-rc1`）只建 image 和打 tag 就停住。
3. `planes`：`docker pull --platform linux/amd64` → `docker save` →
   `python3 scripts/s7n.py apply deployment/sets/planes.json --version <tag>
   --tar /tmp/mftik.tar --wait-seconds 300`（`:341`–`357`）。
4. `api`：`scripts/deploy_prod_compose.sh`，見下。

`apply` 的順序（`scripts/s7n.py:494`）：render → preflight（控制平面版本、每個成員的
機器存在且 reachable、每個 `secret.*`、每個 `volumeMount` 都有宣告，`:348`）→ 補建
volume（`:390`）→ 上傳 artifact（同版不同 digest 直接拒絕，`:207`–`211`）→ 等每個
artifact 進到 `READY`（`:237`）→ `ApplyAssignmentSet`（`:415`）→ 等每個 set 在它
拿到的那個 generation 上報 `Ready`（`:450`）。

### 平面：手動套用與回滾

**對應檔案：** `justfile:121`–`142`。每個 recipe 都要環境裡有 `STRATEGON_API_KEY`。

```sh
just s7n-plan v0.12.0      # dry-run：preflight 加印出會套用什麼，secret 仍是 token
just s7n-planes v0.12.0    # 套用
just s7n-status            # 每個 set 的 phase 與成員
```

回滾就是把舊 tag 再套用一次：catalog 裡已經有的版本不會重傳，所以**回滾不需要
`--tar`**（`scripts/s7n.py:187`–`213`）。同一個 tag 下只改 spec 也是同一條路。

要記得這會重啟平面，所以該成員上的 STS session 會被殺掉再 rebuild（見「OCI 與
`captureStdio`」）。

### infra：手動

```sh
export STRATEGON_API_KEY=...
python3 scripts/s7n.py apply deployment/sets/infra.json \
    --binary nats=/tmp/nats-server   --config nats=deployment/nats/nats.conf \
    --binary redis=/tmp/redis-server --config redis=deployment/redis/redis.conf
```

artifact 版本沒變就不用帶 `--binary`；config 改了才帶 `--config`，而且要先在
`infra.json` 把 `configVersion` 加一。套用之前先確認本機有 `deployment/nats/nats.conf`，
並且 `redis` 的 `configVersion` 已經和線上對齊（見 Redis 那一節的警告）。

### API 層

**對應檔案：** `scripts/deploy_prod_compose.sh`、`deployment/docker-compose.yml`。

`api` job 走 SSH，而不是一個 OCI assignment：API 需要 Traefik 的 `web` network 和
一整組 label，這些 assignment 宣告不了。腳本的順序是 `docker compose down` → 把線上
的檔案備份成 `docker-compose.bak.<shortsha>.yml`（`:42`–`49`）→ scp repo 裡的檔案
上去（`:65`–`70`）→ 備份 `.env` 並改寫 `MFTIK_VERSION` → `pull` →
`--profile tools run --rm migrate`（`alembic upgrade head`）→ `up -d`（`:72`–`85`）→
等前台回 200 且 `/api/auth/me` 回 200 或 401（`:90`–`108`）。備份之後的任何一步失敗，
就把 bak 放回去、把 `MFTIK_VERSION` 改回舊值並 `up`（`:51`–`63`）。

**回滾（只在主機上）：** image 還在 GHCR，所以把 `.env` 的 `MFTIK_VERSION` 改成舊
tag 再 `docker compose up -d` 就夠；要連 compose 檔一起退，就把對應的
`docker-compose.bak.<sha>.yml` 複製回 `docker-compose.yml`。那台機器上已經累積了大量
`.env.bak-*` 和 `docker-compose.bak.*.yml`，沒有任何清理機制。

## 現況（2026-10-01）

**只在主機上。** 要重新確認就跑 `just s7n-status`。

- jp 站台五個 assignment（`nats-jp`、`redis-jp`、`sts-jp`、`td-jp`、`md-jp`）都是
  healthy。
- 平面跑的是 `v0.12.0`，也就是 git tag `v0.12.0`；它落後當時的 `main` 21 個 commit。
  API 和 frontend 的 image tag 同樣是 `v0.12.0`。
- `/opt/mftik/deploy/docker-compose.yml` 和 repo 裡的
  `deployment/docker-compose.yml` 逐位元組相同。
- tw 站台（`yite`）沒有巡檢。上面所有標了「未驗證」的成員，repo 能證明的只有它們被
  宣告過。
