# DataNTs

海量小文件迁移工具。解决 NAS / NFS → NFS 迁移中百万级小文件速度过慢的问题（每文件往返延迟 + 系统调用开销）。支持**断点续传 / 增量迁移**，可有**一台控制机 + 多台 Worker** 分布式协同，数据由 Worker 直连共享存储读写，不经过控制机。

## 核心特性

- 有界 worker 池并行复制，隐藏每文件延迟
- 增量 diff（size + mtime）：只传缺失或变化的文件，中断后重跑零成本续传
- 临时文件 + 原子重命名：崩溃不会留下半写文件
- 带宽限流、有界内存，支撑千万级文件
- 分布式：控制机扫描 + gRPC 调度，Worker 主动拉取任务，失败自动重派

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

RHEL 7.9 自带 Go 太旧（约 1.18）且 glibc 过老（2.17），**不要在目标机上编译**。在一台新机器（或任何装有 Go 1.25+ 的机器）上**静态交叉编译**出二进制，拷到 RHEL 7.9 直接运行，无需安装任何依赖。剩余的 1-6 步见下述文档与下方简版。

### 1. 编译机（Windows / Linux 均可）

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
> 3. `da_worker` 秒退/“finished”**不代表成功**。升级后的控制日志会打印 `progress: done=… failed=…` 与 `report: worker=… failed=…`，失败任务会在下次扫描自动重试。
> 4. 更换二进制后要 `pkill -f da_control; pkill -f da_worker` 杀干净旧进程再启动，否则行为不变。
> 5. 想从零全量重跑：先 `rm -rf /var/lib/migrate/*` 清空任务库，再重启控制机。
> 6. 任务库格式会在本次升级（新增 uid/gid 与 META/DELETE 任务）后变化：旧 `snapshot.dat`/`journal.log` 会被识别为不兼容并自动丢弃，**并且意味着全量重跑**。升级后首次启动相当于从零迁移。

## 主要参数

| 参数             | 默认            | 说明 |
|------------------|-----------------|------|
| `--src`          | 必填            | 源根目录（可为 NFS 挂载点） |
| `--dst`          | 必填            | 目标根目录（可为 NFS 挂载点） |
| `--workers`      | `GOMAXPROCS`    | 并发复制数 |
| `--rate`         | `0` 不限速      | 每秒最大写入字节数 |
| `--retries`      | `3`             | 单文件失败重试次数 |
| `--include-empty-dirs` | `false`   | 同时复制空目录 |
| `--sync-meta`          | `false`   | 同步权限/属主元数据（内容未变时只做 chmod/chown，不重拷；复制时保留属主） |
| `--delete-stale`       | `false`   | 删除目标端多余条目（源端已删除的文件/目录；目录删除需配合 `--include-empty-dirs`） |
| `--ignore`             | -         | 跳过路径片段（可重复） |
| `--log`                | 自动      | 日志文件（默认 `/var/log/da_<进程>.log`；`--log -` 仅输出到 stderr） |

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

## 目录结构

```
cmd/da_migrate/        单节点 CLI
cmd/da_control/        控制机 CLI（扫描 + gRPC 调度）
cmd/da_worker/         Worker CLI
internal/clilog/        日志（默认 /var/log/da_*.log，--log 覆盖）
internal/scanner/     并行目录树遍历
internal/plan/        增量 diff 与任务生成
internal/transfer/    worker 池复制
internal/taskstore/   持久化任务队列（WAL + snapshot）
proto/migrate/v1/     控制面 gRPC 协议
gen/migrate/v1/       buf 生成代码
```