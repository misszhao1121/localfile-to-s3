# rustfs-migrate

把本地 MinIO 桶目录（如 `/data/minio/test`）中的**普通文件**批量迁移到
S3 兼容集群（RustFS）的对象存储中。专为 **数千万级小文件** 设计（已在 4000 万规模
下设计并验证核心路径）。

- 语言：Go 1.22+，单二进制，零外部依赖（SQLite 为纯 Go 实现，无需 CGO）
- 协议：S3 PutObject / HeadObject，路径风格寻址（path-style）

## 核心特性

| 能力 | 说明 |
|---|---|
| 断点续传 | 每个文件的迁移状态存于本地 SQLite（WAL），崩溃/重启后自动跳过已完成 |
| 可中断扫描 | 目录扫描本身可恢复（`scan_done` 标记）；`--rescan` 可重扫增量 |
| 双层重试 | SDK 内置重试 + 应用层指数退避重试（可配），503/SlowDown/网络错误自动处理 |
| 首次自动比对 | `dedup_head` 默认开启：PUT 前先 HEAD，已存在且大小一致的对象直接跳过，适合接手"已传了一部分"的桶 |
| HEAD 校验 | `verify` 模式用 HeadObject 与本地账本对账：缺失/大小不符的自动重新入队 |
| 并发控制 | worker 池 + 每主机连接池上限，避免压垮 5 节点集群 |
| 前缀乱序 | 批内按 key 首段目录分组轮转，避免多目录流在同块磁盘上交叉寻道 |
| 优雅停止 | Ctrl-C 排空在途请求后落盘退出；再按一次强制退出 |
| 源文件只读 | **硬保证**：对源目录仅有两种操作——`readdir` 列目录、`O_RDONLY` 打开读。代码中不存在任何对源的删除/改名/写入调用；状态库/日志若配置在源目录内会直接拒绝启动 |

## 编译

```bash
# Linux（目标运行环境）
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o rustfs-migrate .

# Windows
go build -o rustfs-migrate.exe .
```

本仓库 `dist/` 已带预编译产物：`rustfs-migrate-linux-amd64` 与
`rustfs-migrate-windows-amd64.exe`（Windows 交叉编译验证通过）。

## 快速开始

1. 编辑 `config.example.conf`，保存为 `config.conf`（或在命令行 `-c` 指定路径）。
2. 部署到任意能同时访问源目录与 RustFS 的机器：

```bash
chmod +x rustfs-migrate-linux-amd64
mv rustfs-migrate-linux-amd64 /usr/local/bin/rustfs-migrate

./rustfs-migrate -c /etc/rustfs-migrate/config.conf          # 扫描 + 上传
./rustfs-migrate -c /etc/rustfs-migrate/config.conf stats    # 查看进度
./rustfs-migrate -c /etc/rustfs-migrate/config.conf verify   # HEAD 对账
./rustfs-migrate -c /etc/rustfs-migrate/config.conf retry    # 重试失败项
```

也可以用环境变量：`RUSTFS_MIGRATE_CONFIG=/etc/rustfs-migrate/config.conf ./rustfs-migrate`

## 状态库与恢复

- 状态库：`<state_dir>/migrate.db`（SQLite，WAL 模式）
  - 表 `files`：key、源路径、大小、状态（0=pending, 1=done, 2=failed）、
    尝试次数、错误信息
  - 表 `meta`：`scan_done` 等标志
- 迁移中断后**直接重跑同一条命令**即可续传：已 `done` 的文件不会重复上传。
- `verify` 模式不依赖账本状态：对每个本地文件 HEAD 一次 RustFS，
  大小一致 → 标记完成；不存在/大小不符 → 重新入队上传。
  适合账本丢失或怀疑部分对象丢失时全量对账。
- `reset` 模式清空任务表（慎用）。

## 已通过其他方式传过一部分，会重传吗？

**不会——`dedup_head` 默认开启，migrate 首次运行就自动比对。**

每个待上传文件在 PUT 之前先 HEAD 一次 RustFS：
- 对象已存在且大小与源文件一致 → 标记完成跳过（零上传流量）
- 不存在或大小不符 → 正常 PUT

所以直接跑标准流程即可，不需要先手动 verify：

```bash
./rustfs-migrate -c config.conf    # 自动比对，只传缺的部分
```

纯净桶迁移可以 `dedup_head = false` 关掉，省一次 HEAD。

手动 `verify` 模式仍然保留：适合**怀疑账本不准**时全量对账
（例如迁移中断很久后、或怀疑部分对象丢失），它会对每个本地文件
HEAD 并修正账本，缺失的重新入队。

原理与边界：
- 比对依据是**对象大小**（HEAD 无法廉价比对内容哈希）。正常 S3 客户端
  传输不会出现"大小恰好一致但内容损坏"；若之前的工具可能有截断/损坏
  问题，需要另行抽查。
- HEAD 失败（网络抖动等）不会阻断迁移：该文件照常走 PUT 覆盖写，
  保证最终一致（覆盖写同 key 同内容无副作用）。

## 源目录只读（兜底策略）

程序对源目录的访问被约束为**严格只读**，由三层机制保证：

1. **代码层**：所有源文件读取必须经过 `openSourceReadOnly()`（`sourcefs.go`）——
   唯一的源文件打开点，强制 `O_RDONLY` 并校验路径必须位于 `source_root` 之内；
   全代码库无 `Remove/Rename/Create/WriteFile/Truncate` 等调用作用于源路径。
2. **配置层**：启动时校验 `state_dir`、`log_file` 必须在 `source_root` 之外，
   否则拒绝启动（防止状态库误写进源树）。
3. **运行层**：迁移启动时主动探测源目录是否拒绝写入（尝试创建探针文件），
   若源目录可写则打印 WARNING，建议运维侧挂载只读：
   `mount -o bind,ro /data/minio/test /data/minio/test`
   或对目录 `chmod a-w`。**即使源目录未只读挂载，本工具自身也绝不写入源目录。**

迁移期间的源变化处理：

- 上传时源文件已不存在 → 计为完成（跳过），不报错、不重试
- 扫描时目录/文件消失 → 跳过该项
- 迁移期间新增文件 → 用 `rescan_on_start=true` 重扫补齐
- 上传中的文件被外部修改 → PUT 的是读取那一刻的内容；可用 `verify`
  对账（大小不符自动重传）最终收敛

## 推荐迁移流程（4000 万小文件）

```bash
# 0. 运维兜底：迁移窗口内将源目录挂载/授权为只读（工具自身也绝不写源）
mount -o bind,ro /data/minio/test /data/minio/test

# 1. 预演：先在低峰期小并发试跑，观察 RustFS 集群负载
#    将 workers 从 64 逐步上调至 128~256

# 2. 正式迁移（可反复中断/重跑）
nohup ./rustfs-migrate -c config.conf > migrate.out 2>&1 &

# 3. 迁移完成后对账
./rustfs-migrate -c config.conf verify

# 4. 若有缺失（源文件在迁移期间被删除等），重跑一次 migrate 即可补齐
./rustfs-migrate -c config.conf

# 5. 最终确认 done=total 后，再考虑源目录的归档/清理
./rustfs-migrate -c config.conf stats
```

## 资源与性能参考

- 内存：≈ workers × buffer_size_mib（默认 128×4MiB ≈ 512MiB 峰值）+ SQLite 缓存
- 文件句柄：扫描期最多 64 个目录同时打开 + workers 个上传文件句柄
- 网络：小文件场景瓶颈通常在 RTT 与集群 PUT QPS；调 `workers` 观察
  `rate=xxxx obj/s` 直到集群出现 503/SlowDown 再回调

## 已知取舍

- 单文件整体读入内存后 PUT（小文件最优路径）；超过缓冲的文件按实际大小分配。
  如需大文件（>1GiB）迁移请另行评估分片上传（本项目场景为海量小文件）。
- key 规则：源相对路径原样作为对象 key（可选全局 `prefix`）。
- 源文件在上传时被删除：计为完成（跳过），不报错。


## 代码结构

```
main.go        入口、子命令分发、信号处理
config.go      配置解析（key=value 格式）+ 状态目录/日志与源树隔离校验
store.go       SQLite 状态库（批量写入、游标分页、统计）
scan.go        并发目录扫描（限深、可恢复；仅 readdir）
sourcefs.go    源只读守卫：O_RDONLY 唯一打开点、路径包含校验、只读探测
uploader.go    S3 客户端、上传 worker 池、重试退避、HEAD 校验
run.go         migrate/verify/retry 编排
log.go         进度条与日志
mode.go        子命令常量
inode_*.go     平台相关 inode 读取（仅诊断用）
dbtool/        辅助小工具：直接读取状态库打印失败明细
testdata/      本地 mock S3 服务器（冒烟测试用）
```
