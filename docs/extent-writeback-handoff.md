# Extent 数据面 × kernel writeback cache —— 交接说明

面向接手人（人或 agent）。仓库：`github.com/mem9-ai/drive9`，分支 `feat/extent-juicefs`（PR #913），本文件撰写时 HEAD = `84836610`，工作区干净，Local E2E 绿（最近的代码提交 `7953096b`）。

前置阅读：`docs/extent-todos.md` 的 **P1-9 / P1-10**、`docs/design/extent-content-layout.md` §4.2。本文件是它们的索引与操作手册。

---

## 1. 目标（两项验收，来自用户）

1. **cap off（新默认）**：extent profile 要过全部现有 gate **＋ blackbox `community.sqlite`**（经典路径过不了这个模块，extent 必须能过）。
2. **cap on（`--writeback-cache on`，用户显式声明单写者）**：经典路径与 extent 路径都过全部 gate。

## 2. 当前状态

| 项 | 状态 | 证据 |
|---|---|---|
| 第 2 项 cap on | ✅ **完成并验证** | 实例实测：git-ops 74/74 ×2 profile（含 blobless 的 before-remount 与 post-restore commit）、supervision 51/51、sqlite-correctness 20/20（经典）+ 20/20（extent profile）、on-demand 64/64；CI：Code CI ✓ / Local E2E ✓（`7953096b`） |
| 第 1 项 cap off 的 extent | ❌ **未完成** | cap off 下 `mptester crash01.test` 报 `database is locked`；根因已定量（见 §4），修法已定（见 §7），实现未落地 |

## 3. 已落地的改动（都已推送、CI 绿）

1. `7953096b` **writeback cache 默认关闭**：`auto` 对任何 durability 策略都返回 off；`--writeback-cache on` 成为"本挂载是这些文件唯一写者"的显式声明；`off` 优先于任何策略。内核依据：cap 打开时 `fuse_get_cache_mask()` 对普通文件无条件返回 `STATX_MTIME|STATX_CTIME|STATX_SIZE`，`fuse_change_attributes()` 丢弃服务端值并跳过 `truncate_pagecache`/`auto_inval_data` —— 多写者下这是静默的元数据冻结。
2. `7953096b` **blobless git workspace 的 i_size 修正**：`maybeCorrectKernelCleanNodeSize`（`pkg/fuse/git_workspace.go`）用 `FUSE_NOTIFY_STORE` 推最后一页真实字节 —— 这是 cap 打开时唯一还能走 `fuse_write_update_attr()` 的通道（属性回复会被内核丢弃）。效果：cap on 下 blobless 全绿。
3. `e610339c` **只写 handle 的页合并回读修复**：`openLocalBackingFile`（`pkg/fuse/dat9fs.go`）在 cap 打开时把本地 overlay 的 backing fd 升级为读写（写权限文件回退原 flags）；修掉 `unable to append to '.git/logs/HEAD': Bad file descriptor`。
4. `7953096b` `extentClearSetidAfterWrite` 门控：inode 已知无 SUID/SGID 时不再每次写发一个 meta `GetAttr`。
5. 更早（同一 PR，前序提交）：zombie handles（`4ce69d3a`）、O_APPEND 本地 fd 不用 `WriteAt`（`8ccf7a0a`）等。
6. 文档：`docs/design/extent-content-layout.md` §4.2（cap 语义与三条经典路径规则）、`docs/extent-todos.md` P1-9/P1-10、以及若干纯文档提交（本文件的索引对象）。

## 4. 第 1 项的根因与实测（cap off 下 extent 读路径太贵）

工具：实例上 `/tmp/bigspill2.test`（隔离 spill，单客户端，`mptester`）+ `crash01.test`。

| 配置 | 隔离 spill | `crash01`（wal） |
|---|---|---|
| ext4（对照） | 0.195s | 通过 |
| drive9 extent + cap **on** | 16.2s | **通过**（0 errors / 94 tests） |
| drive9 extent + cap **off**（当前默认） | 71–84s | **FAIL** `database is locked`（其余客户端 10s busy timeout 到期） |

cap off 下 71s 的分解（fork 内插桩，64MB spill）：

| 腿 | 调用次数 | 总耗时 | 单次 |
|---|---|---|---|
| meta 切片查询（`drive9Meta.Read` → `doRead`） | 22 500 | 34.7s | **1.54ms** |
| chunk 数据读（`sliceReader.run` → `dataReader.Read`） | 22 488 | 31.2s | **1.39ms** |

每次读只覆盖 ~10KB。`drain`（`vfs.VFS.Read` 里的 `writer.Flush`）不是主因：p50 仅 0.02ms。

**为什么 cap on 就没事**：内核 page cache 直接吞掉回读 —— 同一份 spill 在 cap on 下整轮只有 **2** 次 FUSE read。cap off 时 `write()` 是 FUSE direct IO，写的页会被内核丢弃，于是每次回读都必须到 daemon。

**为什么 JuiceFS / JuiceFS Cloud 用远端 meta 也能过**（用户的疑问，已分析）：

| JuiceFS 机制 | 我们的嫁接 |
|---|---|
| 每次 meta op 便宜（池化 Redis/SQL/TiKV 或托管服务，亚毫秒） | 一次 HTTP + 服务端一个 TiDB 事务 = **1.54ms/次** |
| 数据读命中客户端本地盘缓存（`--cache-dir`，写透） | 实测 **1.39ms/次**，不是本地命中的量级 |
| 自适应预读把查询摊到越来越大窗口（`fileReader.checkReadahead`，`pkg/vfs/reader.go:423-432`） | 机制在，但 SQLite 页访问半随机、窗口长不起来；实测 ~10KB/次 |

注意：上游同样在**每次写后失效** chunk 映射（`baseMeta.Write` → `m.of.InvalidateChunk`，`pkg/meta/base.go:2168`），我们 fork 也一样（`drive9_engine.go:483`）。所以问题不是"失效"，是**失效之后那次重新查询的价格**。

## 5. 试过但**没有落地**的两条路（重要，避免重复踩坑）

两条都在 fork `~/work/juicefs` 上实现并实测过；**都没有 push，drive9 的 `go.mod` 未动**。

1. **daemon 侧读/写缓存**（`fileWriter` 内 4KiB 页窗口 + 命中即服务，`VFS.Read` 接入）：
   - 效果：命中率 **98.5%**，meta 调用 22 500 → 500，chunk 读 22 488 → 499，spill 71s → **21s**，`crash01` 首次在 cap off 下 **0 errors / 94 tests**；
   - **但**：隔离 spill 报 SQLite `database disk image is malformed`（suite 仍通过 → pattern 相关的静默损坏）。**根因未定位**；
   - 变体（读区间与窗口不相交时跳过 drain）先被否掉：会让 reader 的陈旧 chunk 映射读到旧数据；
   - 代码保存在 fork 分支 `drive9-staged-read-window`（提交 `bc77bccd`，提交信息注明"更快但会损坏 SQLite 数据、未落地"）。
2. **提交后保留 chunk 映射**（把 `parts[i].Slice` 追加进 `m.of` 缓存，代替 `InvalidateChunk`）：2 秒内即损坏。几乎确定是**盲追加导致切片重复/错位**；若要再做，必须按 slice id 合并去重（或干脆保持失效）。

结论：**下一步不要先做缓存**，先做 §7 的四条无 staleness 面的杠杆。

## 6. 环境与工具（都可复用）

- **EC2 测试机**：`ssh -i ~/.ssh/drive9_extent_e2e ec2-user@13.214.129.143`（ap-southeast-1，t3.xlarge）。本地栈：drive9-server on `127.0.0.1:9010`（`DRIVE9_TENANT_PROVIDER=local`，TiDB 容器 + file-mock S3）。**不要**动 `drive9-server`；实例上残留一个早期会话的挂载 `/tmp/p110-mnt-20260910T160847Z`（本次交接时未清理）。
- **harness**：`/home/ec2-user/night/`
  - `env.sh`：PATH（前置 `night/bin`）、私有 `HOME`、`DRIVE9_*`、`provision()`（每次新租户）。
  - `bin/drive9`：PATH shim，把 `--writeback-cache $FUSE_WB_CAP` 注入 `drive9 mount <opts> ...`；支持 `DRIVE9_MOUNT_BIN` 覆盖真实二进制。**只对以裸 `drive9` 启动挂载的脚本生效**（git-ops / git-workspace-* / supervision / sqlite-correctness / `mount.sh` 都属于这类）。
  - `mount.sh <profile> <on|off> <tag> [args]`、`umount.sh <tag>`（挂载点 `night/run/mnt-<tag>`，日志 `night/logs/mount-<tag>.log`）。
  - `sqlite-probe.sh <mount> [cases...]`（mptest wal/delete、threadtest3 walthread2/5、kvtest）。
  - `e2e.sh <profile> <cap> <script> [tag]`、`single.sh <same>`（跑单个 `e2e/*.sh`；支持 `DRIVE9_TREE` 指定树）。
  - `blackbox.sh <profile> <cap> <tag>`（`community.sqlite` 模块，`--server-mode local`）。
  - `accept-fork-patch.sh [tree]`：**一键验收**（打印生效的 replace → 构建 → spill 秒数 → `crash01` 的 `Summary:` → extent cap off/on 与经典 cap off 三个 `sqlite-correctness` 的 `RESULT:`）。
  - `spill2.sh`（TAG 可覆盖）：单独跑 spill + crash01。
- **SQLite 工具**：`/home/ec2-user/bb-work/cache/tools/sqlite/master/{mptester,threadtest3,kvtest}`，脚本在 `.../mptest/*.test`；隔离 spill 脚本 `/tmp/bigspill2.test`。
- **fork 工作副本**：`~/work/juicefs`（dev 机），clone 自 `github.com/mornyx/juicefs`，**有 ADMIN 推权限**；当前在干净基线 `a53df2a9`（= 驱动 `replace` 锁定的提交），实验版在分支 `drive9-staged-read-window`。

## 7. 下一步实施路线（按杠杆排序，均无缓存一致性风险）

| 优先 | 动作 | 目标 | 怎么验证 |
|---|---|---|---|
| 0 | **参照基线**（可选但强烈建议）：原生 JuiceFS + **远端 meta**（TiDB `127.0.0.1:4000` 独立库 + MinIO 容器）+ 同一 `bigspill2.test`/`crash01`，不加 `writeback_cache` | 得到 JuiceFS 的每 op meta/数据读延迟作为预算 | 与我们的 1.54ms/1.39ms 对比 |
| 1 | 让数据读**命中本地 chunk 缓存**（写透保留刚写的 chunk、配额与淘汰配对） | ~0.1ms/读，数据腿 31s → ~2s | fork 计时里 `chunkMs/chunkCalls` |
| 2 | **meta 批量查询**（一次 HTTP 查多个 chunk，或 miss 时预取后续 K 个 chunk 的映射） | meta 腿 34.7s → 2-4s | `metaCalls` 与"每次读覆盖字节数" |
| 3 | **服务端只读 op 去事务化**：chunk 查询不必包在写路径的 `RunExtentMetaOp` 事务里；HTTP 连接复用 | 1.54ms → 亚毫秒 | 服务端 op 耗时分布 |
| 4 | **按块读放大**：miss 时一次取满 `blockSize`(64KB)，后续小读由 `sliceReader` 服务 | 调用次数按比例下降 | 每次读覆盖字节数 → ~64KB |

预期：1+2+3+4 做完可把 66s 压到 ~5-8s，从而 cap off 下 `crash01`（乃至 `community.sqlite`）通过。**验收标准（写死在脚本里）**：spill < 10s、`crash01` `Summary: 0 errors out of 94 tests`、`sqlite-extent-off` / `sqlite-extent-on` / 经典 `sqlite-off` 三个 gate 各 20/20。

落地方式：在 `~/work/juicefs` 上基于 `a53df2a9` 开分支实现 → 推分支 → `go get github.com/mornyx/juicefs@<sha>` 更新 `go.mod` 的 replace（本地试验可用 `go mod edit -replace github.com/juicedata/juicefs=<dir>`，**注意 fork 副本的 module path 是 `github.com/juicedata/juicefs`**，写成 `mornyx/juicefs` 会被校验拒绝）→ 跑 `accept-fork-patch.sh` → 更新 P1-9 → 推送确认 CI。

## 8. 已知坑（都踩过）

1. **rsync 到实例要带 `--delete`**：残留文件会让构建失败，并让整批 gate 误判为失败。
2. **`pkill -f` 会匹配到自己的命令行**：会杀掉 ssh 会话（用 `[x]xx` 括号技巧仍可能匹配自身命令行里其他位置的字符串）；优先按 PID 杀。
3. **判断 cap 是否真的生效，必须看 mount 的 INIT 回复**，不要相信 harness 环境变量（早期 `FUSE_WB_CAP` 曾被误读为空操作，导致 A/B 结论错误）。
4. **本地栈的 API key**：`provision()` 每次都新建租户；CLI 要用 `HOME=<私有目录>` + `DRIVE9_SERVER`/`DRIVE9_API_KEY`，否则会去用 `~/.drive9/config` 里的 dev 上下文（401）。
5. **改 fork 前先确认工作副本干净**（`git -C ~/work/juicefs status`），基线是 `a53df2a9`。
6. **不要在有 4KiB 页语义疑问时依赖"截断/稀疏"行为**：本次实验的两处崩溃都来自"按页缓存 + 部分写"的交互，凡是缓存方案都要先回答"这一页的每个字节是谁写的、什么时候提交的"。

## 9. 未完成 / 未知

- 第 1 项未完成（本文件全部内容都是为它服务）。
- daemon 读缓存的 **corruption 根因未定位**（现象、影响范围、已排除项见 §5）。
- 参照基线未取得：本次交接前把该测量委派给后台子代理，运行多轮未交付，已中断（无报告）。因此 §4 里"JuiceFS 每 op 亚毫秒"是基于其机制与代码的分析推断，**不是本次实测**；建议新接手人把 §7 的第 0 项作为第一个任务，用实测数字确认或修正它。
- 实例上遗留挂载 `/tmp/p110-mnt-20260910T160847Z`（早期会话）。
