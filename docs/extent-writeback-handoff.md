# Extent 数据面 × kernel writeback cache —— 交接说明

面向接手人（人或 agent）。仓库：`github.com/mem9-ai/drive9`，分支 `feat/extent-juicefs`（PR #913），本文件撰写时 fork 已落到
`github.com/mornyx/juicefs@e7a7fe2a`（drive9 `go.mod` 的 `replace` 已指向它）。

前置阅读：`docs/extent-todos.md` 的 **P1-9 / P1-10**（P1-9 里有本次改动的完整测量与两个踩过的坑）、
`docs/design/extent-content-layout.md` §4.2。本文件是它们的索引与操作手册。

---

## 1. 目标（两项验收，来自用户）

1. **cap off（默认）**：extent profile 要过全部现有 gate **＋ blackbox `community.sqlite`**（经典路径过不了这个模块，extent 必须能过）。
2. **cap on（`--writeback-cache on`，用户显式声明单写者）**：经典路径与 extent 路径都过全部 gate。

## 2. 当前状态

| 项 | 状态 | 证据 |
|---|---|---|
| 第 2 项 cap on | ✅ 完成 | 早前实测全绿；本次回归验证：cap on 下 `crash01` `Summary: 0 errors out of 94 tests`（新 fork 亦然） |
| 第 1 项 cap off 的**读路径** | ✅ 已修复并落地 | fork `e7a7fe2a`：隔离 spill **83–87 s → 37 s**；每读一次的 `writer.Flush` 9 479 次/43.2 s → 891 次/15.7 s；`crash01` 由「`database is locked`」变为 **0 errors / 94 tests**（空闲主机 25–33 s） |
| 第 1 项 cap off 的**数据损坏** | ✅ 已消除 | 6 轮单客户端 spill + `integrity_check` 全 `ok`；verify 构建逐字节比对 **30 612 次读、0 不一致** |
| 第 1 项 cap off 的 `crash01` 稳定性 | ⚠️ **未完成** | 主机负载高时 `crash02.subtest` 第 53 行 `--wait all` 超时：崩溃客户端 `--exit 1` 退出时持锁等待 daemon 排空该事务（负载机上 ~35 s），对端 10 s busy timeout 先到期 |
| 第 1 项 cap off 的其余 gate | ⏳ 部分（失败项已确认是既存问题） | `accept-fork-patch.sh`（pin `e7a7fe2a`）：spill **40 s**、`crash01` rc=1（负载机）、**三个 `sqlite-correctness` 各 20/20**。cap off 的 extent gate：`git-ops` 70/74、`supervision` 50/51、`fuse-sqlite-commit-sequence` 失败 —— **同一主机、同一 profile、用未改动的基线二进制跑出完全相同的失败项**，所以不是本次改动的回归；它们同属「重新挂载后读不到刚写入的内容」这一族（缓存失效）。`blackbox community.sqlite` 仍未测 |

## 3. 本次落地的改动（fork `e7a7fe2a`，drive9 侧 `go.mod` + `pkg/extent/lockmem.go`）

`VFS.Read` 过去在读之前无条件 `writer.Flush(inode)`。对 drive9 的 HTTP meta 引擎，这个 flush 是一次元数据提交（HTTP + 一个 TiDB 事务），
并且会 **finish（封存）该 inode 每个 chunk 的在途 slice**；cap off 下每次 4 KiB 写都到 daemon，于是「写一页→读回」就变成新 slice + 新对象 + 新提交。

现在一次读 = **已提交内容 + 写缓冲区里尚未上交的字节**：

1. `pkg/chunk`：`BufferedWriter`（`wSlice.BufferedStart` / `ReadBuffered`）暴露 block writer 还没交给对象存储的字节，按 `WriteAt` 的页布局读，不会凭空造字节。
2. `pkg/vfs/writer.go`：`dataWriter.ReadBuffered` 给出与读区间相交的「仍在缓冲区」区间，以及是否存在「已上传但 meta 还没可见」的字节（那种只能靠 flush）。
3. `pkg/vfs/vfs.go`：命中时把缓冲区字节叠加到 reader 结果上（**叠加而非替换**：reader 覆盖不到的部分仍取自它），需要 flush 时走原路径。
4. `pkg/meta`：`drive9Meta.WriteState` 返回该 inode 的 **pending 提交数** 与 **提交序号**。快路径要求：
   - 读开始前 **没有在途提交**（`pending == 0`）——slice 在提交入队那一刻就离开写缓冲，而 meta 还是旧映射，否则读到旧内容；
   - 读期间 **没有新提交入队**（序号不变）——reader 会缓存它读到的映射，若期间有提交落地，那个窗口就永远不会再被失效，必须主动 `Invalidate` 并回退 flush。

两个条件任一不满足 → 原样 `Flush` + reader。两条都是实测踩出来的（见 P1-9）。

## 4. 关键测量（隔离 spill，64 MB，同一主机）

| | 修改前 | 修改后 |
|---|---|---|
| 每次读的 flush | 9 479 次 / 43.2 s | 891 次 / 15.7 s |
| meta 切片查询（HTTP） | 4 064 次 / 14.4 s | 1 821 次 / 5.9 s |
| chunk 读（reader 窗口） | 8 500 | 2 982 |
| 对象写入 | 202 MB | 132 MB |
| meta 提交 | 3 546 | 992 |
| **spill** | **83–87 s** | **37 s** |
| `crash01` | rc=1（`database is locked`） | `0 errors out of 94 tests` |

剩余开销（37 s 内）：meta 提交 992 × 10.4 ms、对象 PUT 3 943 × ~10 ms、仍需 flush 的 891 次读 × 17.6 ms。

## 5. 环境与工具（都可复用）

- **EC2 测试机**：`ssh -i ~/.ssh/drive9_extent_e2e ec2-user@13.214.129.143`（ap-southeast-1，t3.xlarge）。本地栈：drive9-server on `127.0.0.1:9010`
  （`DRIVE9_TENANT_PROVIDER=local`，TiDB 容器 + MinIO 容器）。**磁盘只有 60 G，务必先 `df -h /`**（见 §8.1）。
- **harness**：`/home/ec2-user/night/`
  - `env.sh`（PATH/私有 HOME/`DRIVE9_*`/`provision()`）、`mount.sh <profile> <cap> <tag>`、`umount.sh <tag>`、`e2e.sh`、`single.sh`、`blackbox.sh`、`sqlite-probe.sh`。
  - `accept-fork-patch.sh [tree]`：一键验收（spill 秒数 + `crash01` 的 `Summary:` + 三个 `sqlite-correctness` 的 `RESULT:`）。
  - 本次新增（诊断用，值得保留）：
    - `instr.sh <cap> <tag> spill|crash01|both`：用 `INSTR_BIN`（默认 `/home/ec2-user/drive9-instr/bin/drive9`）挂载并跑用例，末尾汇总
      `DRIVE9_JFS_STATS` 计数器（该计数器版 fork 已随清理移除，脚本仍可用于跑用例）。
    - `repro.sh <tag> <cap> <rounds> [bin]`：单客户端 crash01 形状负载 + `PRAGMA integrity_check`，逐轮打印 `ok` / `malformed`。**这是最快的数据损坏信号。**
    - `crashrepro.sh`（`kill -9` 未提交事务 + 恢复）、`mpsmall.sh`（缩小版 crash01，便于慢构建也能跑完）、`mpj.sh <tag> <journalmode|delete|wal> <bin>`（对比日志模式）。
    - `hang-diag.sh`：复现锁超时并抓进程栈 / daemon strace。
    - `start-server.sh` + `server.env`：本地 e2e server 的重启方式（`server.env` 由旧进程 `/proc/<pid>/environ` 抓取）。
- **构建树**：`/home/ec2-user/drive9-instr`（`go.mod` 用本地 path replace 指向 `/home/ec2-user/juicefs-fork`，改动迭代最快）、
  `/home/ec2-user/drive9-acc`（用 `go.mod` 里 pin 的 fork commit，验收用）。构建约 40 s。
- **fork 工作副本**：`~/work/juicefs`（dev 机），clone 自 `github.com/mornyx/juicefs`，**有 ADMIN 推权限**；基线 `a53df2a9`，本次分支 `drive9-extent-read-from-write-buffer`（= `e7a7fe2a`）。
- **SQLite 工具**：`/home/ec2-user/bb-work/cache/tools/sqlite/master/{mptester,threadtest3,kvtest}`；脚本 `mptest/*.test`；spill 脚本 `/tmp/bigspill2.test`。

## 6. 下一步（按杠杆排序）

1. **让 `crash01` 在负载机上也能 `rc=0`**：失败点是崩溃客户端退出时的排空延迟（持锁 → 对端 10 s busy timeout）。可查/可做的方向：
   - `VFS.Release`/`Close` 的 flush 是否必须等对象 PUT 完成（writeback 语义下本地 stage 已是持久化边界）；
   - 客户端 `--exit 1` 时内核 close 是否先释放 POSIX 锁再等 FLUSH（若顺序相反，锁被无谓地多持有一段排空时间）；
   - 把排空本身变便宜：现在 992 次 meta 提交 × 10.4 ms + 3 943 次 PUT × ~10 ms 是 37 s 的大头。
2. **跑齐 cap-off gate**：`git-ops`（74×2 profile）、`supervision`（51）、on-demand、`blackbox community.sqlite`、三个 `sqlite-correctness`（各 20/20）。
3. 参照基线问题**已结案**：原生 JuiceFS 1.4.1 + 同一套本地 TiDB + MinIO，在 cap off 下 `crash01` **同样失败**（300 s 超时；其 meta op 平均 5.7 ms，且 `setlk` 也走 meta）。所以「JuiceFS 每 op 亚毫秒」的推断在本环境不成立，不需要再花时间复现基线。
4. 若还要继续压 spill：`forceUpload`/`FlushTo` 的提前上传（上传后 block 不可变，重写必须新开 slice）与 `commitThread` 的提交时机是最大的两个结构性杠杆。

## 7. 未完成 / 未知

- `crash01` 在**空闲主机**上 0 errors/94（cap off，新 fork），在**负载主机**上仍会 `--wait all` 超时；判定标准（用户口径）应写清是「0 errors」还是「rc=0 且无 malformed」。
- **重新挂载后的可见性**：`git-ops` 的 post-restore 三项、`supervision` 的 `fresh mount reads seeded cache probe bytes`、`commit-sequence` 的 `remounted fingerprint matches` 在基线二进制上同样失败（已 A/B），是 cap-off extent 的另一族既存问题（挂载级缓存失效），与本次读路径改动无关，但 requirement #1 要过全部 gate 就还得处理它。
- cap off 下 WAL 模式的**多客户端崩溃恢复**仍会打印 `database disk image is malformed`（同一负载下 rollback journal 模式 0 条）：
  在**本 fork 之前**就存在（是否由我的改动引入无法直接 A/B，因为旧二进制在该负载下根本跑不完第一轮就锁超时）。
  它出现在 `crash02.subtest` 的第 2 轮，且该轮末尾的 `integrity_check` 仍报 ok —— 症状像跨进程 WAL/SHM 视图不一致，值得单独查（`-shm` 走 transient local overlay + `FOPEN_KEEP_CACHE`，见 `pkg/fuse`）。
- `accept-fork-patch.sh` 的落盘验收（pin `e7a7fe2a`）：spill 40 s、`crash01` rc=1 67 s、三个 `sqlite-correctness` 20/20。
- **空闲主机复测（负载 0.2）**：spill 38 s；`crash01` 两次都是 rc=1（56 s / 84 s，`--wait all` 超时 + 对端 `database is locked`），但 **corrupt=0**（无 `malformed`）。所以「崩溃客户端退出前持锁排空事务」是稳定复现的最后一关，不是抖动；而早前两次 `0 errors out of 94 tests` 出现在**无 corruption 修复**的中间版本上，接手人不要把它当作当前基线。

## 8. 已知坑（都踩过）

1. **磁盘满会毁掉一切结论**：`/home/ec2-user/jfs-ref`（参照基线实验）一度涨到 31 G，把 60 G 根盘打满 → TiDB 出现 20 分钟不提交的事务、
   drive9-server 卡在 provision、mount 无响应、还伴随 SQLite `malformed` 假象。`df -h /` 先看，必要时清掉旧的 `jfs-ref`/实验树（`sudo rm -rf`，minio 数据是 root 所有）。
2. **`pkill -f <pattern>` 会杀掉自己的 ssh 会话**（pattern 出现在自己的命令行里）：用 `pgrep` 取 PID 再 `kill`，或把整段逻辑放进实例上的脚本文件里跑。
3. **`go get github.com/mornyx/juicefs@<sha>` 必失败**（fork 的 module path 是 `github.com/juicedata/juicefs`）。正确做法：
   `go mod edit -replace github.com/juicedata/juicefs=github.com/mornyx/juicefs@v1.4.2-0.<YYYYMMDDHHMMSS>-<sha12>`，
   时间戳必须与 git 提交时间一致——写错时 `go mod tidy` 会告诉你 expected 值（照抄即可）。
4. **判断 cap 是否真的生效，只看 mount 的 INIT 回复**，不要相信 harness 环境变量。
5. **本地栈的 API key**：`provision()` 每次新建租户；CLI 必须 `HOME=<私有目录>` + `DRIVE9_SERVER`/`DRIVE9_API_KEY`。
6. **长命令经 ssh 容易被中断**（本机侧连接被断过一次）：长时间用例放成 `setsid nohup bash /path/script.sh > /tmp/x.out 2>&1 &`，再轮询输出。
7. **server 卡死后的重启**：`/home/ec2-user/night/start-server.sh`（读 `server.env`，过滤掉 `BASH_FUNC_*` 这类多行环境量）；`server.env` 是从旧进程 `/proc/<pid>/environ` 抓的。
8. **测量前先确认主机不忙**：`uptime` 负载 3.5+ 时同一用例会慢 3–4 倍，`crash01` 的结论会翻转。
