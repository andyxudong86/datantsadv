# DataNTs

海量小文件迁移工具。解决 NAS / NFS → NFS 迁移中百万级小文件速度过慢的问题（每文件往返延迟 + 系统调用开销）。支持**断点续传 / 增量迁移**，可有**一台控制机 + 多台 Worker** 分布式协同，数据由 Worker 直连共享存储读写，不经过控制机。

## 核心特性

- 有界 worker 池并行复制，隐藏每文件延迟
- 增量 diff（size + mtime）：只传缺失或变化的文件，中断后重跑零成本续传
- 临时文件 + 原子重命名：崩溃不会留下半写文件
- 带宽限流、有界内存，支撑千万级文件
- 分布式：控制机扫描 + gRPC 调度，Worker 主动拉取任务，失败自动重派
- 按 IP 圈定参与迁移的 Worker：Worker 页把节点保存进 Worker 池（池外 Worker 领不到任何任务）；池内且已连线的节点显示"准备接收任务"

## 构建

需要 **Go 1.25+**（只需要在一台构建机上装 Go，目标服务器不用装）。三个可执行文件：

```sh
go build -o da_migrate ./cmd/da_migrate   # 单节点迁移
go build -o da_control ./cmd/da_control   # 分布式：控制机
go build -o da_worker  ./cmd/da_worker    # 分布式：Worker
```

依赖已全部 **vendor** 进仓库，离线/无外网机器构建：

```sh
export GOFLAGS=-mod=vendor
go build -o da_migrate ./cmd/da_migrate
```

## 部署到 Red Hat 7.9

> **从 Gitee 下载代码开始的完整逐步安装文档见 [docs/install-rhel7.9.md](docs/install-rhel7.9.md)**（含 RHEL 7.9 上装 Go、离线安装、systemd、分布式、故障排查）。
> **部署完成后的验收/冒烟说明见 [docs/verify-rhel7.9.md](docs/verify-rhel7.9.md)**（10 分钟自动体检 + Web 控制台/分布式/失败重试验证清单）。

RHEL 7.9 自带 Go 太旧（约 1.18）且 glibc 过老（2.17），**不要在目标机上编译**。在一台新机器（或任何装有 Go 1.25+ 的机器）上**静态交叉编译**出二进制，拷到 RHEL 7.9 直接运行，无需安装任何依赖。剩余的 1-6 步见下述文档与下方简版。

### 1. 编译机（Windows / Linux 均可）

用仓库自带脚本一键产出 RHEL 7.9 静态产物到 `dist/rhel7/`（含 `go version -m` 自检）：

```sh
sh scripts/build-rhel7.sh
```

或按下面命令手动编译：

```sh
# Windows PowerShell:
$env:GOOS="linux"; $env:GOARCH="amd64"; $env:CGO_ENABLED="0"
go build -o da_migrate ./cmd/da_migrate
go build -o da_control ./cmd/da_control
go build -o da_worker  ./cmd/da_worker
```

```sh
# Linux / macOS:
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_migrate ./cmd/da_migrate
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_control ./cmd/da_control
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_worker  ./cmd/da_worker
```

> `CGO_ENABLED=0` 产出纯静态二进制，不依赖目标机 glibc 版本，RHEL 7.9 / Rocky 9 等 x86_64 系统通用。

### 2. 拷贝到服务器

```sh
scp da_migrate da_control da_worker root@<服务器IP>:/tmp/
ssh root@<服务器IP>
```

### 3. 在 RHEL 7.9 上部署

```sh
mkdir -p /usr/local/datants /var/lib/migrate /mnt/source /mnt/destination
mv /tmp/da_migrate /tmp/da_control /tmp/da_worker /usr/local/datants/
chmod +x /usr/local/datants/*
ln -s /usr/local/datants/da_migrate /usr/local/bin/da_migrate
ln -s /usr/local/datants/da_control /usr/local/bin/da_control
ln -s /usr/local/datants/da_worker  /usr/local/bin/da_worker
```

> RHEL 7.9 默认无 systemd？有。也可以用 `nohup ... &` 后台运行。

### 4. 单节点迁移（最常用）

```sh
da_migrate --src /mnt/source --dst /mnt/destination --include-empty-dirs
# 限速（每秒 100 MB）：
da_migrate --src /mnt/source --dst /mnt/destination --rate 104857600
# 中断后续传：重新运行同一命令即可，已完成的文件自动跳过
```

systemd 服务 `/etc/systemd/system/da_migrate.service`：

```ini
[Unit]
Description=DataNTs Data Migration
After=network.target

[Service]
ExecStart=/usr/local/datants/da_migrate --src /mnt/source --dst /mnt/destination --include-empty-dirs
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```sh
systemctl daemon-reload
systemctl enable --now da_migrate
```

### 5. 分布式（1 控制机 + N Worker，可选）

所有节点挂载**同一个共享源/目标**（NFS 等），路径必须一致。控制机：

```sh
nohup da_control --src /mnt/source --dst /mnt/destination \
  --listen 0.0.0.0:50051 --state /var/lib/migrate --include-empty-dirs & 
```

每台 Worker（防火墙放行控制机 50051 端口）：

```sh
nohup da_worker --controller <控制机IP>:50051 --id workerA --batch 64 --capacity 8 &
```

日志默认自动写入 `/var/log/da_control.log`、`/var/log/da_worker.log`（各二进制自带，无需 shell 重定向）；用 `--log` 可改路径（`--log -` 表示仅 stderr）。

控制机重启后重新运行同一命令，即可**断点续传**。

> **分布式实战要点**
>
> 1. 只有控制机需要放行 50051（`firewall-cmd --permanent --add-port=50051/tcp && firewall-cmd --reload`）；Worker 是主动外连，不用开端口。
> 2. 控制机启动初期在做扫描/入库，**还没开监听**，此时 Worker 报 `connection refused` 是正常的——新版 Worker 会自动指数退避重试，可先于控制机启动。
> 3. `da_worker` 秒退/“finished”代表着它把当前能领到的任务做完了（非报错）。升级后的控制日志会打印 `progress: done=… failed=…` 与 `report: worker=… failed=…`，失败任务会在下次扫描自动重试。想让 worker 常驻（做完也接着待命），加 `--idle-poll 3s`。
> 4. 更换二进制后要 `pkill -f da_control; pkill -f da_worker` 杀干净旧进程再启动，否则行为不变。
> 5. 想从零全量重跑：先 `rm -rf /var/lib/migrate/*` 清空任务库，再重启控制机。
> 6. 任务库格式会在本次升级（新增 uid/gid 与 META/DELETE 任务）后变化：旧 `snapshot.dat`/`journal.log` 会被识别为不兼容并自动丢弃，**并且意味着全量重跑**。升级后首次启动相当于从零迁移。

### 5.1 Web 控制台（可选）

`da_control` 内嵌一个**无依赖的 Web 控制台**（原生 JS，无需 Node/构建，离线可用）：

- **仪表盘**：实时进度、速率、失败数（SSE 推送）；暂停/续跑/停止/重扫按钮
- **迁移历史**：每次运行的耗时、文件/字节、平均速率
- **任务明细**：按状态筛选（默认只看失败，便于排查）
- **Worker**：在线状态与各 Worker 累计完成量；配合 `--worker-bin` 可在**控制机上直接拉起/停止本机 worker**（无需登服务器敲命令行）

`da_control` 支持两种启动方式，推荐第 ① 种：

**① 待命模式（推荐，GUI 全程控制）**——不带 `--src/--dst`，控制机只起 gRPC + Web，
Worker 池与任务都在控制台配置。"**Worker 页**"是配置 Worker 的唯一入口：每行一个节点（`IP 并发 批次`，如 `10.0.0.7 8 64`）保存为 Worker 池，只有池内且已连线的节点会进入"**准备接收任务**"状态；点"本机启动 Worker"拉起本机 worker 时自动把 `127.0.0.1` 加入池使其就绪（无需再 ssh 上去跑 da_worker）；其它机器先按命令行启动 da_worker 并把自己的 IP 加入池即可。"启动迁移"表单只填源/目标与迁移选项——任务只会派发给池中"准备接收任务"的节点，未保存池时派发给所有在线 Worker。待命模式（不带 `--src/--dst`）重启后**不会**自动重跑上一轮任务——残余任务库会保持冻结，直到你在界面上新发起一次迁移：

```sh
nohup da_control \
  --state /var/lib/migrate --listen 0.0.0.0:50051 \
  --http 0.0.0.0:8080 --web-auth admin:change-me \
  --worker-bin /usr/local/datants/da_worker &
# 浏览器打开 http://<IP>:8080 → Worker 页配置/启动 Worker 池 → 仪表盘填源/目标 → 点"开始迁移"
```

> 控制机上的 worker 由控制进程拉起后**常驻空闲轮询**（`--idle-poll`），可在"开始迁移"之前先启动；Worker 页可随时"停止"。
> 其它机器上的 worker 仍需命令行启动：`nohup da_worker --controller <IP>:50051 --id workerA &`（默认做完一整轮即退出；若也想像控制机拉起的那样常驻，加 `--idle-poll 3s`）。

**② 传统模式（启动即开跑，systemd/脚本友好）**——带上 `--src/--dst`，控制机一启动就自动开始第一次迁移：

```sh
nohup da_control --src /mnt/source --dst /mnt/destination \
  --listen 0.0.0.0:50051 --state /var/lib/migrate \
  --http 0.0.0.0:8080 --web-auth admin:change-me &
```

> `--src/--dst` 必须成对出现（只给一个会报错）；两个都不给即待命模式。
> 已开跑的迁移不可重复启动（GUI 会禁用"开始"按钮、API 返回 409）；待其完成后（状态回到空闲）才能发起下一次。

在生产环境请务必：

- 用 `--web-auth user:pass` 开启 HTTP Basic 认证，否则任何能访问 8080 端口的人都能远程启停迁移；
- 若控制机出口于公网，别把 8080 直接暴露；用 ssh 隧道 `ssh -L 8080:127.0.0.1:8080 root@<控制机IP>` 或反代加 TLS 更稳妥；
- 本机用浏览器直接打开 `http://127.0.0.1:8080` 即可，无需安装任何东西。

> 仅从本机访问且机器很安全时可以省略 `--web-auth`（默认监听 `127.0.0.1`，不影响迁移流程）。

## 主要参数

| 参数             | 默认            | 说明 |
|------------------|-----------------|------|
| `--src`          | -               | 源根目录（可为 NFS 挂载点）。da_migrate 必填；da_control 可选——提供则启动即自动开跑，与 `--dst` 一起省略则进入待命模式（只起 gRPC + Web，由 Web/API 发起迁移） |
| `--dst`          | -               | 目标根目录（可为 NFS 挂载点）。规则同 `--src`，必须成对出现 |
| `--workers`      | `GOMAXPROCS`    | 并发复制数 |
| `--rate`         | `0` 不限速      | 每秒最大写入字节数 |
| `--retries`      | `3`             | 单文件失败重试次数 |
| `--include-empty-dirs` | `false`   | 同时复制空目录 |
| `--sync-meta`          | `false`   | 同步权限/属主元数据（内容未变时只做 chmod/chown，不重拷；复制时保留属主） |
| `--delete-stale`       | `false`   | 删除目标端多余条目（源端已删除的文件/目录；目录删除需配合 `--include-empty-dirs`） |
| `--ignore`             | -         | 跳过路径片段（可重复） |
| `--log`                | 自动      | 日志文件（默认 `/var/log/da_<进程>.log`；`--log -` 仅输出到 stderr） |
| `--http`               | `127.0.0.1:8080` | Web 控制台监听地址（`da_control` 专属；空串关闭控制台） |
| `--web-auth`           | -         | Web 控制台 HTTP Basic 认证，格式 `user:pass`（`da_control` 专属） |
| `--worker-bin`         | -         | 本机 `da_worker` 可执行文件路径；设置后在 Web 控制台 **Worker 页可一键拉起/停止本机 worker**（`da_control` 专属，空串禁用该功能） |
| `--idle-poll`          | `0`       | 空闲轮询间隔：>0 时 worker 做完也没任务后**常驻不退出**并持续取任务（可先于迁移启动）；`0` 保持"做完即退出"（`da_worker` 专属） |

> **`--sync-meta` / `--delete-stale` 是控制机扫描参数**（`da_migrate` 也有）：它们决定扫描时生成哪些增量任务。
>
> - **`--sync-meta`（控制机）**：扫描检测 mode/uid/gid 变化并生成 META 任务（内容不变只做 chmod/chown，不重拷）。不开启则这类变化被忽略。
> - **`--delete-stale`（控制机）**：生成 DELETE 任务镜像删除目标端多余条目。目录级删除需 `--include-empty-dirs`。
> - **Worker 只需执行任务，不配置这两个开关**：`--delete-stale` 在 worker 上**不存在**，传了会报 unknown flag。Worker 的 `--sync-meta` 仅控制复制新文件时是否保留属主；META 任务的 chown 不受其控制，始终尝试写入目标 uid/gid——worker 无权限 chown 时 META 任务报错，控制机 `report:` 日志可见。
> - 日志默认写 `/var/log/da_<进程>.log`，`--log` 可覆盖。

## 测试

```sh
go build ./...
go vet ./...
go test ./... -timeout 180s
```

除常规单元测试外，`internal/e2e` 还包含两组真实验证：

- **端到端冒烟**：临时编译本机版 `da_control`/`da_worker`，起真实进程做一次完整迁移（含空目录、删多余文件、Web 控制台、SSE、历史），校验落盘文件与任务清单；
- **RHEL 7.9 产物校验**：交叉编译三个 linux/amd64 静态二进制，用 `go version -m` 断言 `GOOS=linux`/`GOARCH=amd64`/`CGO_ENABLED=0`，防止产物悄悄退化为动态链接。

## 目录结构

```
cmd/da_migrate/        单节点 CLI
cmd/da_control/        控制机 CLI（扫描 + gRPC 调度 + 内嵌 Web 控制台）
cmd/da_worker/         Worker CLI
internal/clilog/        日志（默认 /var/log/da_*.log，--log 覆盖）
internal/scanner/     并行目录树遍历
internal/plan/        增量 diff 与任务生成
internal/transfer/    worker 池复制
internal/taskstore/   持久化任务队列（WAL + snapshot）
internal/webapi/      Web 控制台后端 REST/SSE API
internal/webui/       内嵌 Web 控制台（SPA + go:embed）
internal/e2e/         端到端冒烟 + 交叉编译产物校验
scripts/              构建脚本（scripts/build-rhel7.sh）
proto/migrate/v1/     控制面 gRPC 协议
gen/migrate/v1/       buf 生成代码
```
