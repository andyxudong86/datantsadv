# Red Hat Enterprise Linux 7.9 安装部署指南（从 Gitee 下载代码开始）

> 适用：RHEL / CentOS 7.9（x86_64），无 VPN、纯国内网络。
> 目标：把 DataNTs 部署到 RHEL 7.9 上，完成 NFS/本地目录迁移，支持断点续传与分布式多节点。
>
> **部署完成后，用 [verify-rhel7.9.md](verify-rhel7.9.md) 做 10 分钟验收**（单节点、Web 控制台、分布式、失败重试对照清单）。

---

## 0. 前置条件核对

在开始之前，先在服务器上确认三件事：

```bash
# 1) 系统版本必须是 7.9（glibc 2.17、内核 3.10，满足 Go 运行要求）
cat /etc/redhat-release
uname -r
ldd --version | head -1

# 2) CPU 架构必须是 x86_64（本项目只发布 amd64 二进制）
uname -m

# 3) 国内网络可访问以下地址（拉代码 + 下载 Go 工具链）
#    - https://gitee.com        （代码仓库）
#    - https://mirrors.aliyun.com （Go 下载镜像，替代被墙的 golang.org）
```

> 若服务器**完全无外网**（内网隔离），跳过节 2、3、4 中的联网步骤，改用「离线安装」小节。
> 最低要求：能访问 `gitee.com` 或能拿到代码压缩包，能下载 Go 1.25+ 工具链。

---

## 1. 配置国内 yum 源（解决 mirrorlist.centos.org 无法解析）

CentOS 7 已于 2024-06 停止维护，官方 `mirrorlist.centos.org` 已下线，`yum` 报错：

```
Could not retrieve mirrorlist http://mirrorlist.centos.org/...
14: curl#6 - "Could not resolve host: mirrorlist.centos.org; Unknown error"
```

解决：把仓库文件里的官方地址替换为**国内镜像**（阿里云 / 腾讯云任选其一）。

### 方式 A：阿里云（推荐）

```bash
# 备份默认源
mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak

# 下载阿里云的 CentOS 7 源
curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo

# 清理缓存并重建
yum clean all
yum makecache
```

### 方式 B：腾讯云

```bash
mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak
curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.cloud.tencent.com/repo/centos7_base.repo

yum clean all
yum makecache
```

> 说明：
> - 阿里云 / 腾讯云镜像仍保留 CentOS 7 的仓库（含 EOL 后的归档 vault），上面的配置开箱即用。
> - 若服务器本身就是 CentOS Base 意外缺失，可用阿里云备用地址重新下载上面的文件。
> - 需要 EPEL 时（装某些开发包）：
>   ```
>   yum install -y epel-release
>   sed -i 's|^\(baseurl=\).*$|\1https://mirrors.aliyun.com/epel/$releasever/$basearch/|; s|^#baseurl=http://download.fedoraproject.org.*$|baseurl=https://mirrors.aliyun.com/epel/$releasever/$basearch/|' /etc/yum.repos.d/epel.repo
>   yum clean all && yum makecache
>   ```

验证：

```bash
yum repolist
# 应能列出 base / updates / extras，不再是 mirrorlist.centos.org
```

**RHEL 7.9（正版红帽）用户注意**：RHEL 自带源走 subscription-manager，不报此错；
报这个错说明是 CentOS 7，按上面配置即可。

---

## 2. 安装基础工具（git / wget / tar）

```bash
yum install -y git wget tar
```

> RHEL 7.9 自带 git 版本较旧（1.8.x），clone 国内 Gitee 仓库没有问题；
> 如果遇到 TLS/证书问题，跳到下面的「方式 B：下载 zip」绕过 git。

---

## 3. 从 Gitee 下载代码

仓库地址：`https://gitee.com/andy0105/datants.git`

### 方式 A：git clone（推荐，方便以后 `git pull` 更新）

```bash
cd /opt
git clone https://gitee.com/andy0105/datants.git
cd /opt/datants

# 验证代码已拉全：必须能看到 vendor/ 目录（依赖已打包，离线可编译）
ls -d vendor
git log --oneline -1
```

### 方式 B：下载 zip 压缩包（没有 git 或 git 报错时）

```bash
cd /opt
wget https://gitee.com/andy0105/datants/repository/archive/master.zip -O datants.zip
yum install -y unzip
unzip datants.zip
# 解压后目录名通常带前缀，改名为 datants 便于操作
mv datants* datants
cd /opt/datants

ls -d vendor
```

---

## 4. 下载并安装 Go 1.25 工具链

RHEL 7.9 官方 YUM 源里的 Go 是 1.18（太老，无法编译本项目），所以**手动安装**国内镜像上的 Go 1.25：

```bash
cd /opt
# 下载 go1.25.14（阿里云镜像，国内可达）
wget https://mirrors.aliyun.com/golang/go1.25.14.linux-amd64.tar.gz

# 校验文件完整（SHA-256 应与官方一致）
echo "a21ae5633a269bcd7e90cf767e48225633795e99d831742cbf3397064fee7712  go1.25.14.linux-amd64.tar.gz" | sha256sum -c -

# 解压到 /usr/local
tar -C /usr/local -xzf go1.25.14.linux-amd64.tar.gz
```

配置环境变量（写入 `/etc/profile.d/go.sh`，对所有用户生效）：

```bash
cat > /etc/profile.d/go.sh <<'EOF'
export PATH=$PATH:/usr/local/go/bin
export GOTOOLCHAIN=local
export GOFLAGS=-mod=vendor
export GOPROXY=off
EOF

# 立即生效
source /etc/profile.d/go.sh

# 验证
go version        # 必须 >= go1.25
```

> - `GOTOOLCHAIN=local`：禁止 Go 尝试联网下载其它版本工具链。
> - `GOFLAGS=-mod=vendor` + `GOPROXY=off`：强制使用仓库自带 vendor 依赖，**编译全程不走外网**。

---

## 5. 编译三个可执行文件

在 `/opt/datants` 目录下执行：

```bash
cd /opt/datants

# 一次性编译三个命令
go build -o da_migrate ./cmd/da_migrate
go build -o da_control ./cmd/da_control
go build -o da_worker  ./cmd/da_worker

# 验证产物
ls -lh da_migrate da_control da_worker
file da_migrate da_control da_worker   # 应显示 ELF 64-bit，静态链接（statically linked）
```

> 默认关闭 CGO，产物为纯静态二进制，不依赖目标机 glibc 版本；
> 装在新机器/其它 RHEL7 上可直接复制运行。

---

## 6. 部署到系统目录

```bash
mkdir -p /usr/local/datants
mv /opt/datants/da_migrate /opt/datants/da_control /opt/datants/da_worker /usr/local/datants/
chmod +x /usr/local/datants/*

# 软链到 PATH，方便直接敲命令
ln -sf /usr/local/datants/da_migrate /usr/local/bin/da_migrate
ln -sf /usr/local/datants/da_control /usr/local/bin/da_control
ln -sf /usr/local/datants/da_worker  /usr/local/bin/da_worker

# 验证
da_migrate --help
```

---

## 7. 准备源/目标目录并挂载共享存储（如有 NFS）

```bash
mkdir -p /mnt/source /mnt/destination

# 若源/目标是 NFS 共享，先挂载（IP 按实际替换）：
# mount -t nfs 192.168.1.100:/nfs/share /mnt/source
# mount -t nfs 192.168.1.101:/nfs/share /mnt/destination
# 分布式模式要求所有节点挂载的路径一致
```

---

## 8. 单节点迁移（最常用）

```bash
# 前台运行一次测试
da_migrate --src /mnt/source --dst /mnt/destination --include-empty-dirs

# 限速（每秒 100 MB）：
da_migrate --src /mnt/source --dst /mnt/destination --rate 104857600

# 断点续传：中断后重新执行同一命令即可，已迁完文件自动跳过

# 日志默认自动写 /var/log/da_migrate.log，无需重定向；--log - 可改仅输出到终端。
```

### 后台运行：systemd 服务

创建 `/etc/systemd/system/da_migrate.service`：

```ini
[Unit]
Description=DataNTs Data Migration
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/datants/da_migrate --src /mnt/source --dst /mnt/destination --include-empty-dirs
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now da_migrate
systemctl status da_migrate
```

---

## 9. 分布式多节点（可选：1 控制机 + N Worker）

所有节点都完成前 5 步，并**挂载同一共享存储**（路径一致）。

### 9.1 控制机（Controller）

```bash
mkdir -p /var/lib/migrate
nohup /usr/local/datants/da_control \
  --src /mnt/source --dst /mnt/destination \
  --listen 0.0.0.0:50051 --state /var/lib/migrate --include-empty-dirs &

# 日志默认自动写 /var/log/da_control.log（无需 shell 重定向），
# 用 --log <路径> 可改，--log - 仅输出到 stderr。
# 重启控制机后重新执行同一命令即可断点续传
```

> **待命模式（可选）**：`--src/--dst` 可直接省略——控制机只起 gRPC + Web 控制台，
> 在 `http://<IP>:8080` 的"启动迁移"表单里填源/目标再发起（`--src/--dst` 必须成对出现，
> 都不给 = 待命；只给一个会报错）。适合希望完全通过 Web 界面管控的运维方式。

### 9.2 每台 Worker

```bash
nohup /usr/local/datants/da_worker \
  --controller <控制机IP>:50051 --id workerA --batch 64 --capacity 8 &

# 日志默认自动写 /var/log/da_worker.log（无需 shell 重定向）。
# 注意：da_worker 没有 --delete-stale / --sync-meta（删除与元数据任务由控制机扫描生成，
# worker 只执行收到的任务）；--log - 可改为仅 stderr 输出。
```

### 9.3 防火墙放行控制机端口

```bash
# 控制机上执行（RHEL7 用 firewalld）
firewall-cmd --permanent --add-port=50051/tcp
firewall-cmd --reload
```

> 只有**控制机**需要放行 50051。Worker 是**主动外连**控制机，不用开端口；本机防火墙默认即可。

### 9.4 分布式运行注意事项（实战踩坑）

以下都是真实部署中遇到过的坑，按频率排列：

| 现象 | 原因与解决 |
|------|-----------|
| `da_worker` 报 `connect: connection refused`，几秒后没反应 | **控制机刚启动时还没监听**：`da_control` 是先扫描/入库、后开监听，启动初期连接被拒是正常的。新版 Worker 会自动指数退避重试，等控制机就绪即可；如果你在用旧版，请升级 |
| `da_worker` 报 `connect: no route to host`，但能 ping 通控制机 | **控制机防火墙没放行 50051**：按 9.3 节放行。`no route to host` 通常是 REJECT 策略，不是网络不通 |
| 两台 Worker 一个能跑、一个 "finished" 秒退 | Worker 退出 ≠ 成功。**失败的任务被控制机记录但旧版不打日志**。升级后控制日志会打印 `progress: done=… failed=…` 和 `report: worker=… failed=…`，据此定位失败原因 |
| 迁移"只复制了几个目录"就停 | 看 `da_control.log` 的 `report:` 行中失败任务的具体错误（多数是源路径打不开/权限/挂载只读）。新版**失败任务会在下次扫描自动重试** |
| 换了新编译的二进制但行为没变 | **老进程没杀干净**：`pkill -f da_control; pkill -f da_worker` 后再启动。日志自动追加写 `/var/log/da_*.log`，重启后记得 `tail` 或按时间戳区分新旧日志 |
| 想从头重新全量迁移 | 清空任务库再重启：`rm -rf /var/lib/migrate/snapshot.dat /var/lib/migrate/journal.log`，否则控制机会按增量语义跳过已完成任务 |

### 9.5 任务库（先清后跑）

```bash
# 首次部署或想全量重跑时，务必清空任务库再启动控制机，
# 否则旧任务状态（done/failed）会让新迁移跳过部分文件。
rm -rf /var/lib/migrate/*
pkill -f da_control
```

---

## 10. 更新（拉取新版本并重新编译）

```bash
cd /opt/datants
git pull                     # 若用 zip 方式，重新下载解压覆盖即可
source /etc/profile.d/go.sh
go build -o da_migrate ./cmd/da_migrate
go build -o da_control ./cmd/da_control
go build -o da_worker  ./cmd/da_worker
mv -f da_migrate da_control da_worker /usr/local/datants/
systemctl restart da_migrate    # 若以 systemd 方式运行
```

---

## 11. 离线安装（服务器完全无外网时）

在**有外网的机器**上提前准备好两个文件，拷贝到内网服务器：

1) Go 工具链：`wget https://mirrors.aliyun.com/golang/go1.25.14.linux-amd64.tar.gz`
2) 代码：`git clone https://gitee.com/andy0105/datants.git`（自带 vendor 依赖）

拷贝进服务器后，与第 4、5 节步骤相同（tar 解压 Go、配置环境变量、go build），
全程不联网，因为依赖已打包在 `vendor/` 中。

---

## 常见问题排查

| 现象 | 原因与解决 |
|------|-----------|
| `yum` 报 `mirrorlist.centos.org` 无法解析 | CentOS 7 已 EOL，官方源下线。按第 1 节换成阿里云/腾讯云国内源 |
| `yum install` 慢/失败 | 换国内 yum 源（第 1 节） |
| git clone 证书/TLS 报错 | 用「方式 B」下载 zip 绕过 git，或先 `yum update -y git` |
| `go version` 低于 1.25 | 第 4 步 PATH/环境变量没生效，重新 `source /etc/profile.d/go.sh` |
| 编译报 `dial tcp ... proxy.golang.org` | 未设置 `GOFLAGS=-mod=vendor`/`GOPROXY=off`，见第 4 步 |
| `file` 显示动态链接 | 编译时未设 `CGO_ENABLED=0`，重编即可 |
| 迁移后目标文件 mtime 有偏差 | 老 NFS/文件系统时间精度问题，可加 `--force` 全量重拷 |
| 增量误判反复重拷 | 同上，配合 `--force` 规避 |
| 改过权限/属主，增量却没反映到目标 | `--sync-meta`/`--delete-stale` 是**控制机**参数：控制机扫描时生成 META/DELETE 任务，worker 无法自行开启。控制机加 `--sync-meta` 才检测权限/属主变化；属主/组变更还需 worker 有目标端 root/相应权限，否则 META 任务失败会在 `da_control.log` 的 `report:` 里看到 |
| 源端删了文件，目标端还在 | 属正常（默认不做镜像删除）；要同步删除请给**控制机**加 `--delete-stale`，目录级删除需再加 `--include-empty-dirs` |
| 升级后首次启动说任务库不兼容/全量重跑 | 任务库格式带 magic 版本号，旧库被自动丢弃；如需保留增量需提前全量 `--force` 重拷完成**再升级** |
| `da_migrate` 只建目录、不复制文件（单核机器） | 旧版 scanner 死锁，升级到最新版即修复；已写入回归测试 |
| `da_worker` 秒退 `connection refused` | 控制机启动初期未监听，正常；新版 Worker 自动重试，升级即可 |
| 分布式只复制了部分目录/文件 | 升级后看 `da_control.log` 的 `report:` 失败明细；控制机防火墙放行 50051（9.3 节） |
| 迁移不幂等、跳过文件 | 首次/全量重跑前清空任务库（9.5 节） |
