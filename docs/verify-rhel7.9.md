# RHEL 7.9 部署验证说明（验收清单）

> 前置：已完成 `docs/install-rhel7.9.md` 的部署（三个二进制已在 `/usr/local/datants/`，命令已软链到 PATH），并且**目标机可挂载源/目标共享存储**。
> 目标：用一组可复制的命令，在 10 分钟内证明部署可用、各功能符合预期，避免"装上=能用"的假象。

---

## 0. 五分钟快速验收（自动化冒烟）

把以下几行当作部署后必跑的"体检"，全部符合即视为通过：

```bash
# 三个二进制存在、可执行、静态链接
ls -l /usr/local/datants/da_*
file /usr/local/datants/da_* | grep -c "statically linked"   # 期望 3

# 每种二进制能打印帮助（说明依赖库/glibc 没问题）
da_migrate --help >/dev/null && echo "migrate OK"
da_control --help >/dev/null && echo "control OK"
da_worker  --help >/dev/null && echo "worker OK"
```

输出 `3` 与三行 `OK` 即完成前提验收。下面的章节逐个功能展开验证。

---

## 1. 产物与二进制验证

### 1.1 静态链接（不依赖 glibc 版本）

```bash
file /usr/local/datants/da_control
# 期望：ELF 64-bit LSB executable, x86-64, statically linked
```

> 若显示 `dynamically linked`：说明编译时没有 `CGO_ENABLED=0`，换用仓库
> `scripts/build-rhel7.sh` 的产物（`dist/rhel7/`），或重新按 README 第 1 节编译。

### 1.2 构建设置留档（可选，需要目标机也有 Go）

```bash
go version -m /usr/local/datants/da_control
# 期望包含三行：
#   build   GOOS=linux
#   build   GOARCH=amd64
#   build   CGO_ENABLED=0
```

---

## 2. 单节点迁移验证（da_migrate）

### 2.1 准备一小套测试数据

```bash
BASE=/root/dnts-smoke
rm -rf $BASE && mkdir -p $BASE/src/sub $BASE/src/emptydir $BASE/dst

# 100 个小文件 + 一个较大文件 + 一个空目录
for i in $(seq 1 100); do echo "file $i" > $BASE/src/file-$i.txt; done
seq 200000 > $BASE/src/sub/blob.bin
mkdir -p $BASE/src/emptydir
```

### 2.2 首次全量迁移（含空目录）

```bash
da_migrate --src $BASE/src --dst $BASE/dst --include-empty-dirs --log -
```

验证：

```bash
# 目标树与源完全一致（含空目录 emptydir 被创建）
diff -rq $BASE/src $BASE/dst && echo "TREE MATCH"     # 应输出 TREE MATCH
# 空目录确实存在于目标端
test -d $BASE/dst/emptydir && echo "EMPTYDIR OK"
# 大文件字节数一致
wc -c $BASE/src/sub/blob.bin $BASE/dst/sub/blob.bin
```

### 2.3 断点续传 / 增量幂等

```bash
# 已跑过的相同命令再跑一遍
da_migrate --src $BASE/src --dst $BASE/dst --include-empty-dirs --log -
# 期望：数秒内结束（日志不再大量复制），任务库语义是"跳过已完成文件"

diff -rq $BASE/src $BASE/dst && echo "STILL MATCH"
```

### 2.4 镜像删除（--delete-stale）验证

```bash
echo "must be removed" > $BASE/dst/stale.txt

da_migrate --src $BASE/src --dst $BASE/dst --delete-stale --log -

test ! -e $BASE/dst/stale.txt && echo "STALE REMOVED"   # 应输出 STALE REMOVED
```

> `--delete-stale` 也可与 `--sync-meta` 组合，检测权限/属主变化并生成 META 任务。
> 若对 root 属主变更做验证，需在 worker 端有相应权限，否则 META 任务失败会记录在
> 控制机 `report:` 日志中（详见第 4.4 节）。

---

## 3. Web 控制台验证（da_control，M3 交付）

控制台内嵌在 `da_control` 中（无 Node/Java 依赖，浏览器打开即可）。它复用第 4 节的分布式启动方式；**推荐与分布式一起验证**。

### 3.1 启动（推荐待命模式：迁移由 GUI 发起）

控制机不带 `--src/--dst` 启动，只起 gRPC + Web 控制台；在浏览器"启动迁移"表单里填源/目标目录后开跑（`--src/--dst` 必须成对出现，都不给 = 待命）：

```bash
da_control \
  --state /var/lib/migrate --listen 0.0.0.0:50051 \
  --http 0.0.0.0:8080 --web-auth admin:CHANGE-ME \
  --log -
```

传统模式（带 `--src/--dst`）则启动即自动开跑，适合 systemd/脚本一键拉活。

### 3.2 用 curl 冒烟（不需要浏览器）

```bash
# 未带凭据 → 401
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/          # 401
# 带正确凭据 → 200
curl -s -o /dev/null -w '%{http_code}\n' -u admin:CHANGE-ME http://127.0.0.1:8080/   # 200

# REST API（均需 -u admin:CHANGE-ME）
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/status | head -c 400
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/workers
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/runs
curl -s -u admin:CHANGE-ME "http://127.0.0.1:8080/api/tasks?status=failed"
```

### 3.3 实时进度（SSE）

```bash
# 持续推送 event: status 帧，Ctrl-C 退出
curl -N -u admin:CHANGE-ME http://127.0.0.1:8080/api/events
```

> 连接后应立刻收到第一帧 `event: status`，随后每 ~2 秒一帧。若只收到首帧后停更，
> 说明迁移完成（无活动 run），属正常；有活动 run 时帧内 `state` 与 `tasks` 持续变化。

### 3.4 浏览器访问

本机浏览器打开 `http://<IP>:8080`，输入账号密码后应看到四个标签页：

| 页面 | 内容 | 验证点 |
|------|------|--------|
| 仪表盘 | 状态徽标、进度条、任务/失败/速率、控制按钮（暂停/续跑/停止/重扫） | 运行中有实时变化 |
| 迁移历史 | 每次运行：ID、耗时、文件/字节、平均速率 | 完成过 run 后会列出，状态为"完成" |
| 任务明细 | 默认自动过滤 `failed`，可切到全部 | 能按状态筛选 |
| Worker | 每台 Worker 在线状态、地址、并发、累计完成量 | 与 `/api/workers` 一致 |

> 在目标机上 `--http 0.0.0.0:8080` 但浏览器打不开：放开防火墙 8080
> （`firewall-cmd --permanent --add-port=8080/tcp && firewall-cmd --reload`）
> 或用 SSH 隧道 `ssh -L 8080:127.0.0.1:8080 root@<IP>` 避免暴露公网。

---

## 4. 分布式验证（da_control + da_worker，M2 交付）

> 控制机与所有 Worker 必须挂载**同一共享存储**且路径一致。

### 4.1 启动顺序

```bash
# 控制机（先启动；扫描/入库阶段尚未开监听，worker 稍后自己重试也能连上）。
# 推荐待命模式：不带 --src/--dst，首次迁移在 Web 控制台发起；
# 若带 --src/--dst 则启动即自动开跑。
da_control \
  --state /var/lib/migrate --listen 0.0.0.0:50051 \
  --http 0.0.0.0:8080 --web-auth admin:CHANGE-ME \
  --log /var/log/da_control.log &

# 每台 Worker（主动外连，无需开端口）
da_worker --controller <控制机IP>:50051 --id workerA --capacity 8 --batch 64 \
  --log /var/log/da_worker.log &
```

### 4.2 Worker 上线验证

```bash
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/workers
# 期望："connected": true 的 worker 出现（控制台"Worker"页同步可见）
```

> 短暂 `connection refused` 是控制机启动初期未监听所致，worker 会自动指数退避重试，
> 属正常现象，不要据此判定失败。

### 4.3 全流程完成验证

```bash
# 控制机日志出现"run complete"即为完成
tail -f /var/log/da_control.log
# 期望：progress: done=… failed=0，随后 run … complete: done=… failed=0

# 目标树与源一致（在挂载共享存储的任意节点上执行）
diff -rq /mnt/source /mnt/destination && echo "TREE MATCH"

# 历史里有状态 done 的一次 run
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/runs
ls /var/lib/migrate/history/ | tail        # 持久化的 run_<id>.json
```

### 4.4 失败任务可见与自动重试

制造一个 worker 读不到的文件（用非 root 用户跑 worker 时最直接）：

```bash
echo "secret" > /mnt/source/hidden/noperm.txt
chmod 000 /mnt/source/hidden/noperm.txt      # 非 root worker 无法读取 → 复制失败
```

验证：

```bash
# 任务明细内可见一条 failed（控制台"任务明细"默认就展示失败）
curl -s -u admin:CHANGE-ME "http://127.0.0.1:8080/api/tasks?status=failed"
# 控制机日志看失败明细
grep "report:" /var/log/da_control.log | tail
```

> 失败任务会被保留在任务库；**下一次增量扫描自动重试**，也可在控制台点"重扫"
> 立即触发（`curl -X POST /api/rescan`）。无活动 run 时执行 `rescan`/`stop` 会得到
> HTTP 409，这是正常防护，可用来验证控制命令链路。

### 4.5 暂停 / 续跑 / 停止验证

```bash
curl -s -X POST -u admin:CHANGE-ME http://127.0.0.1:8080/api/pause
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/status  # state: "paused"
curl -s -X POST -u admin:CHANGE-ME http://127.0.0.1:8080/api/resume
curl -s -u admin:CHANGE-ME http://127.0.0.1:8080/api/status  # state: "running"

# 停止当前 run（历史会记一条 stopped）
curl -s -X POST -u admin:CHANGE-ME http://127.0.0.1:8080/api/stop
```

---

## 5. systemd 服务化验证（可选）

`/etc/systemd/system/da_control.service`：

```ini
[Unit]
Description=DataNTs Controller
After=network.target

[Service]
Type=simple
# 待命模式：迁移在 Web 控制台发起；如需开机即迁移可补上 --src/--dst
ExecStart=/usr/local/datants/da_control --state /var/lib/migrate --listen 0.0.0.0:50051 --http 0.0.0.0:8080 --web-auth admin:CHANGE-ME
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now da_control
systemctl status da_control        # active (running)
```

---

## 6. 验收清单

| 编号 | 验证项 | 命令 / 位置 | 通过标准 |
|------|--------|------------|----------|
| 1 | 二进制齐全 | `ls /usr/local/datants/` | da_migrate、da_control、da_worker 三个存在且可执行 |
| 2 | 静态链接 | `file ...` | 三个均为 `statically linked` |
| 3 | 命令能启动 | `--help` | 三命令均正常打印帮助 |
| 4 | 单节点迁移 | diff -rq | 源/目标树一致，空目录已建 |
| 5 | 增量幂等/断点续传 | 重复执行迁移 | 秒级完成、内容一致 |
| 6 | 镜像删除 | `--delete-stale` | 目标多余文件被删除 |
| 7 | Web 控制台 | 浏览器 / curl | 页面 4 个标签页可用；API 返回 JSON |
| 8 | Web 认证 | `curl` 不带凭据 | 401；带正确凭据 200 |
| 9 | SSE 实时进度 | `curl -N /api/events` | 首帧即 `event: status` |
| 10 | Worker 上线 | 控制台 Worker 页 / `/api/workers` | 所有 worker `connected: true` |
| 11 | 分布式完成 | 控制机日志 | `run complete: done=… failed=0` |
| 12 | 失败任务 | 控制台任务明细 / `/api/tasks?status=failed` | 失败可见，重扫后自动重试 |
| 13 | 历史记录 | `/api/runs` + `history/run_*.json` | 每次运行都有状态（done/failed/stopped） |
| 14 | 暂停/续跑/停止 | 控制台按钮 / curl | 状态在 paused↔running 间切换；stop 记 stopped |
| 15 | 防火墙 | 控制机 | 50051 放行（worker 已连上即证明） |

> 1–9 必测；10–15 分布式部署必测；其余按需。

---

## 7. 常见"验证不通过"速查

| 现象 | 原因与处理 |
|------|-----------|
| `file` 显示动态链接 | 编译未设 `CGO_ENABLED=0`；改用 `scripts/build-rhel7.sh` 产物重装 |
| `da_worker` 连不上 / 秒退 | 控制机启动初期未监听（正常，自动重试）；或防火墙未放行 50051 |
| 控制台打不开（本机能 200，服务器上浏览器不行） | 8080 未放行 / `--http` 只监听 127.0.0.1；用 SSH 隧道最稳 |
| 控制台 401 | `--web-auth` 账号或密码配错；或密码含特殊字符未正确转义 |
| 控制台页面一直"连接中" | SSE 未通：检查反向代理是否关闭缓冲（SSE 需 `X-Accel-Buffering: no`） |
| Worker 页 0 在线 | worker 没起来 / 控制机 50051 未放行 / 高可用场景 worker 与控制机不同网段不通 |
| `/api/tasks?status=failed` 为空但总失败数不为 0 | 失败任务属于已完结 run，明细以当前任务库为准；历史里看 run 的 `result.failed` |
| 分布式"只迁了一部分" | 看 `da_control.log` 的 `report:` 失败明细行 |
| 控制台按钮点了没反应 | 无活动 run 时部分命令返回 409（如 stop/rescan），属正常防护 |

---

## 8. 验证通过后须知

- **日志自动落盘**：控制机 `/var/log/da_control.log`、worker `/var/log/da_worker.log`；
  `--log -` 改为仅 stderr，`--log <路径>` 可改目录。
- **断点续传**：任何时刻重启控制机，重跑同一启动命令即续传（任务库在 `--state` 目录）。
- **全量重跑**：先清任务库 `rm -rf /var/lib/migrate/*` 再启动控制机，否则按增量语义跳过已完成文件。
- **更新**：`git pull` 后按 `docs/install-rhel7.9.md` 第 10 节重编三个二进制替换即可，
  不需要动配置；替换后务必 `pkill -f da_control; pkill -f da_worker` 杀干净旧进程。
