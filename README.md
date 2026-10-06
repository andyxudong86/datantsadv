# DataNTs

海量小文件迁移工具，解决 NAS / NFS → NFS 迁移中百万级小文件速度过慢的问题。支持**断点续传 / 增量迁移**，可**一台控制机 + 多台 Worker** 分布式协同；数据由 Worker 直连共享存储读写，不经过控制机。

## 特性

- 有界 worker 池并行复制，隐藏每文件延迟
- 增量 diff（size + mtime）：只传缺失或变化的文件，中断后重跑零成本续传
- 临时文件 + 原子重命名：崩溃不留半写文件
- 带宽限流、有界内存，支撑千万级文件
- 分布式：控制机扫描 + gRPC 调度，Worker 主动拉任务，失败自动重派
- 内嵌 Web 控制台（原生 JS、无依赖）：实时进度 / 暂停 / 停止 / 历史 / 任务明细 / Worker 管理，全程 GUI 操作

## 构建

需要 Go 1.26+（仅构建机需要，目标机不用装）：

```sh
go build -o da_migrate ./cmd/da_migrate   # 单节点迁移
go build -o da_control ./cmd/da_control   # 分布式：控制机
go build -o da_worker  ./cmd/da_worker    # 分布式：Worker
```

依赖已全部 vendor 进仓库，离线构建：`export GOFLAGS=-mod=vendor`（Windows 为 `$env:GOFLAGS="-mod=vendor"`）。

## 快速开始

### 单节点

```sh
da_migrate --src /mnt/source --dst /mnt/destination --include-empty-dirs
```

中断后续传：重新运行同一命令即可，已完成的文件自动跳过。

### 分布式（1 控制机 + N Worker）

所有节点需挂载**同一个共享源/目标**（NFS 等），路径一致。

控制机：

```sh
nohup da_control --state /var/lib/migrate --listen 0.0.0.0:50051 \
  --http 0.0.0.0:8080 --web-auth admin:change-me &
```

Worker（防火墙放行控制机 50051 端口）：

```sh
nohup da_worker --controller <控制机IP>:50051 --id workerA --batch 64 --capacity 8 &
```

日志默认写 `/var/log/da_control.log`、`/var/log/da_worker.log`，可用 `--log` 改路径。想让 worker 做完也常驻待命，加 `--idle-poll 3s`。

### Web 控制台

`da_control` 内嵌无依赖 Web 控制台。浏览器打开 `http://<控制机IP>:8080`：

- **仪表盘**：实时进度 / 速率 / 失败数（SSE），暂停 / 续跑 / 停止 / 重扫
- **Worker 页**：配置 Worker 池（每行 `IP 并发 批次`）；配合 `--worker-bin`、`--ssh-user`、`--ssh-key` 可界面一键拉起 / 停止节点上的 da_worker
- 不带 `--src/--dst` 启动即**待命模式**，任务完全由界面发起；带上则启动即自动开跑

生产环境务必用 `--web-auth user:pass` 开启 Basic 认证。

## RHEL 7.9 部署

目标机 glibc 过老（2.17），不要在目标机上编译。在构建机静态交叉编译后拷贝运行：

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_migrate ./cmd/da_migrate
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_control ./cmd/da_control
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o da_worker  ./cmd/da_worker
```

> 纯静态二进制不依赖目标机 glibc 版本，RHEL 7.9 / Rocky 9 等 x86_64 系统通用。

完整逐步部署（在 RHEL 7.9 上装 Go、systemd、离线安装、故障排查）见 **[docs/install-rhel7.9.md](docs/install-rhel7.9.md)**；部署后的验收/冒烟清单见 **[docs/verify-rhel7.9.md](docs/verify-rhel7.9.md)**。

## 常用参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--src` / `--dst` | - | 源 / 目标根目录，必须成对（`da_migrate` 必填；`da_control` 都省略则进入待命模式） |
| `--workers` | `GOMAXPROCS` | 并发复制数 |
| `--rate` | `0` 不限 | 每秒最大写入字节数 |
| `--retries` | `3` | 单文件失败重试次数 |
| `--include-empty-dirs` | `false` | 同时复制空目录 |
| `--sync-meta` | `false` | 同步权限 / 属主（内容未变只 chmod/chown，不重拷） |
| `--delete-stale` | `false` | 删除目标端多余条目（目录删除需配合 `--include-empty-dirs`） |
| `--ignore` | - | 跳过路径片段（可重复） |
| `--log` | 自动 | 日志文件（默认 `/var/log/da_<进程>.log`，`-` 仅 stderr） |
| `--http` | `127.0.0.1:8080` | Web 控制台监听地址（`da_control`，空串关闭） |
| `--web-auth` | - | Web Basic 认证 `user:pass`（`da_control`） |
| `--worker-bin` | - | 本机 `da_worker` 路径；设置后 Worker 页可一键拉起本机 worker（`da_control`） |
| `--idle-poll` | `0` | 空闲轮询间隔；>0 时 worker 做完也常驻待命（`da_worker`） |

## 测试

```sh
go vet ./...
go test ./... -timeout 600s
```

`internal/e2e` 另含两组真实验证：端到端冒烟（真实进程完整迁移，校验落盘文件与任务清单）与 RHEL 7.9 静态产物校验（`go version -m` 断言 `CGO_ENABLED=0`）。

## 目录结构

```
cmd/da_migrate/        单节点 CLI
cmd/da_control/        控制机 CLI（扫描 + gRPC 调度 + 内嵌 Web 控制台）
cmd/da_worker/         Worker CLI
internal/scanner/      并行目录树遍历
internal/plan/         增量 diff 与任务生成
internal/transfer/     worker 池复制
internal/taskstore/    持久化任务队列（WAL + snapshot）
internal/webapi/       Web 控制台后端 REST/SSE API
internal/webui/        内嵌 Web 控制台（go:embed）
internal/e2e/          端到端冒烟 + 交叉编译校验
scripts/               构建脚本（build-rhel7.sh）
proto/migrate/v1/      控制面 gRPC 协议
gen/migrate/v1/        buf 生成代码
```