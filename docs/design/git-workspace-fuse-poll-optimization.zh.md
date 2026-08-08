# FUSE mount：Git Workspace 完全按需发现

**状态：** 最终方案  
**日期：** 2026-08-09  
**范围：** FUSE 侧 git-workspace 发现与刷新；消除「未使用 `drive9 git --fast`」时的 `/v1/git-workspaces*` 空转请求  
**非范围：** overlay 写路径、checkpoint、hydrate 协议本体  
**关联：** [git-fast-clone-workspace.md](./git-fast-clone-workspace.md)、[pack-unpack-profile-spec.md](./pack-unpack-profile-spec.md)  
**English version:** [git-workspace-fuse-poll-optimization.md](./git-workspace-fuse-poll-optimization.md)

---

## 1. 问题

当前行为：overlay profile 挂盘会设 `LocalRoot`，从而 `EnableGitWorkspaces=true`。几乎每个 Lookup 都会进入 `gitWorkspaceForPath` → `ensureGitWorkspaces`，并以 **1s TTL** 调用 `ListGitWorkspaces`。即使列表一直为空，只要有 FS 活动就会持续请求后端。

用户期望：**若从未用 `drive9 git --fast` 注册工作区，则完全不产生 `/v1/git-workspaces*` 请求**；同时仍支持同机 live 注册、以及跨沙箱 remount 恢复。

---

## 2. 目标与非目标

### 2.1 目标

| ID | 说明 |
| --- | --- |
| **G0** | index 确认不存在（404/空）且无本地 armed 信号时，任意 FS 活动下 **`/v1/git-workspaces*` = 0** |
| **G1** | 同 LocalRoot 上 live `--fast` 后，**下一次 FS op** 即可进入 git 层（活跃 mount 上通常亚秒级） |
| **G2** | 新沙箱 / 新 LocalRoot remount：远程 index 有本 mount 可见条目时，可恢复 clean + overlay + git-state |
| **G3** | 武装后无空转 list；刷新由本地 marker、index 变更、SSE 等事件驱动 |
| **G4** | 错误路径有退避与 singleflight，禁止 op 级请求风暴 |

### 2.2 非目标

- 不默认关闭「将来可用 `--fast`」的能力（默认 DORMANT，可武装）。  
- 不承诺：mount 时 index 已 404 确认后，运行中另一机器才首次 `--fast` 时，**不 remount** 也能看见（见 §6 产品边界）。  
- 不把远程 index 当作 runtime 权威源（禁止仅凭 index 加载 tree/overlay）。  
- 不为 `/.drive9/` 引入强制隐藏（与 packs 同级：dotdir 约定）。  
- 不提供 `drive9 git reindex` 用户命令。

---

## 3. 结论摘要

| 场景 | `/v1/git-workspaces*` | 其它远程 IO |
| --- | --- | --- |
| 只 mount、从未 `--fast`、index 404 | **0** | mount 后 **≤1×** FS `Stat` index |
| 同机 live `--fast` | 本地 armed 后按需 list / tree / overlay | 无周期空转 |
| 新沙箱 remount 且 index 有条目 | **1×** list + 按需 tree/overlay | **1×** FS Stat/Get index |
| 已 ARMED，跨机增删 workspace | list 仅在 index 信号变化时 | 可有节流 FS Stat 或 SSE |
| 删光 workspace 且 index 为空 | 回 DORMANT → **0** | — |

---

## 4. 核心模型：DORMANT → ARMED

```text
DORMANT（默认）
  · 不调用 /v1/git-workspaces*
  · gitEntry 全部 miss（与无 git 层一致）
  · 可做：本地 armed 检查；mount 后异步 FS Stat index
        │
        │ 本地 armed 信号，或远程 index 有本 mount 可见条目
        ▼
ARMED
  · ListGitWorkspaces 构建 runtime（DB 权威）
  · tree / overlay 事件驱动刷新
  · 禁止空列表周期 poll
        │
        │ list 为空且无本地信号 / index 确认空
        ▼
DORMANT
```

| 概念 | 含义 |
| --- | --- |
| `EnableGitWorkspaces` | 允许进入状态机（与 `LocalRoot != ""` 等挂盘能力相关） |
| `armed` | 已允许调用 git-workspaces API |
| `dormantConfirmed` | 本 mount 已确认「无远程 index / 无相关条目」，不再主动 Stat index，直到本地 armed |

---

## 5. 元数据

### 5.1 远程 index（跨沙箱）

**路径（租户绝对，与 `/.drive9/packs` 同构）：**

```text
/.drive9/git-workspaces/index.json
```

- 读写使用 client 绝对 remote path（`StatCtx` / `ReadStream` / `WriteCtxConditionalWithRevision`），**禁止**经 FUSE `remotePath` 二次拼接。  
- 条目中 `root_path` 为租户绝对路径；mount 按自身 `RemoteRoot` 过滤不可见条目。  
- **fs_scoped** 若读不到租户根 `/.drive9/`：无法靠 index 武装；可用 `--git-workspaces=on` 或扩大 scope。

**schema（仅存在性，字段宜少）：**

```json
{
  "version": 1,
  "updated_at": "2026-08-09T00:00:00.000Z",
  "workspaces": [
    {
      "workspace_id": "ws_xxx",
      "root_path": "/repo/",
      "workspace_kind": "main"
    }
  ]
}
```

规则：

- 不在 index 中放 `repo_url` 等恢复全量字段。  
- **武装后总是 `ListGitWorkspaces`（或 Get by id）构建 runtime**；index 只回答「是否应 arm」。  
- 不存在或 `workspaces` 为空 → 可 `dormantConfirmed`。  
- 写入：单次整体覆盖 + revision CAS；空列表时 CAS 写空文档（避免无条件 delete 与并发 upsert 竞态）。  
- 不需要单独的 `epoch` 文件；用 revision / mtime / `updated_at` 即可。

### 5.2 本地 LocalRoot（同机 live）

```text
<LocalRoot>/git-workspaces/armed
<LocalRoot>/git-workspaces/refresh/<id>
<LocalRoot>/git-workspaces/deleted/<id>
```

**目录级武装信号（不得仅依赖「已加载 id」的 per-id 扫描）：**

```text
localArmSignal =
    exists(armed)
    OR refresh/ 下存在任意文件
    OR refresh/（或 armed）mtime 相对上次扫描前进
```

- `deleted/` 用于 list 后隐藏与失效，不单独作为 arm 条件。  
- 仅 **同一 LocalRoot** 的 mount 共享本地信号；不同 `--local-root` / 凭证 → 走远程 index 路径。  
- 已 ARMED 时本地 mtime 再前进（例如同 mount 第二次 `--fast` 注册新 id）→ **force list**。

### 5.3 写入方

| 事件 | 远程 index | 本地 |
| --- | --- | --- |
| `git clone --fast` | upsert 条目 | touch armed + refresh |
| `git worktree add --fast` | upsert linked | 同上 |
| `git worktree remove --fast` | 移除条目；空则写空 index | deleted marker |
| FUSE 删除 workspace 根 | **必须** 更新 index | 本地 deleted |
| SDK / 服务端 Upsert·Delete | 推荐服务端自动维护 index | 可选 |

**CLI 成功路径顺序：**

```text
1. UpsertGitWorkspace + ReplaceGitTree + git-state
2. 更新远程 index（CAS，失败 → 整命令 fail）
3. 本地 armed / refresh（失败 → fail）
4. 向用户打印 success
5. hydrate（可选，可长时间；与发现无关）
```

---

## 6. 行为规格

### 6.1 DORMANT 热路径

```text
if !EnableGitWorkspaces → miss
if localArmSignal → arm + list（按需）
elif armed → ARMED 路径
else → miss（不 list）
```

未武装时禁止：`ListGitWorkspaces`、tree、overlay、git-state API；禁止因路径含 `.git` 而 force list。

### 6.2 Mount 时远程 index

1. Mount 完成且 SSE watcher 启动后，**异步** probe index（不阻塞 mount 成功）。  
2. **404 / 空 / 过滤后无本 mount 条目** → `dormantConfirmed`，此后 0 git-workspaces API（直至本地 armed）。  
3. **有相关条目** → `armed`，再 `ListGitWorkspaces` 建 runtime。  
4. **网络 / 5xx / 解析失败** → 不确认 dormant；指数退避重试 Stat（上限约 30s）。  
5. **401 / 403** → 视为永久不可读，确认 dormant 并停止 probe 循环（避免 fs_scoped 空转）。

### 6.3 武装后刷新

| 事件 | 动作 |
| --- | --- |
| 本地 armed / refresh / deleted 信号变化 | force list（受退避与节流约束） |
| 路径命中已加载 workspace | 内存 runtime |
| 远程 index revision/mtime 变化（节流 Stat，默认 60s，有 FS 活动时） | force list |
| SSE：index 路径变更（`EnableGitWorkspaces` 时） | force list / re-arm |
| 无事件 | **零** list |

list / tree / overlay 失败：

- 有旧非空 snapshot → 保留；  
- 从未成功加载 → `loaded=false`，退避结束后可再试；  
- force 请求在退避期间记为 sticky（`pendingForce` + generation），成功航班若中途又有 force，不得吞掉补投。

并发 list：singleflight；waiter 在需要时再入。

### 6.4 产品边界

| 场景 | 是否可见 git 层 |
| --- | --- |
| 从未 `--fast` | 否 |
| 同 LocalRoot live `--fast` | 是（下一次 FS op） |
| remount / 新沙箱，index 已写 | 是（mount probe） |
| 已 ARMED，跨机增删 | 是（≤60s Stat 或 SSE） |
| 已 dormantConfirmed，跨机才首次 `--fast` | **否**（需 remount 或同 LocalRoot 本地信号） |

### 6.5 存量与逃生

不提供 `reindex` 子命令。已有 DB workspace、尚无 index 时：

1. 再走任意会写 index 的路径（`--fast`、worktree、FUSE 删除等）；或  
2. 挂盘 `--git-workspaces=on` 直接 list（调试/救急）；或  
3. 服务端 Upsert/Delete 自动维护 index，并可做部署期后台回填。

---

## 7. 关键决策

| # | 决策 | 选择 |
| --- | --- | --- |
| 1 | 默认是否 poll | 否，DORMANT |
| 2 | Mount 是否读 index | 是，异步 ≤1 次 FS Stat |
| 3 | index 路径 | 租户绝对 `/.drive9/git-workspaces/index.json` |
| 4 | index 角色 | 仅存在性；runtime 只靠 List/Get |
| 5 | 同机 armed | 目录级信号；禁止仅 per-id 已加载扫描 |
| 6 | CLI index/本地写失败 | 整命令 fail |
| 7 | CLI 写序 | index + local 先于 success/hydrate |
| 8 | Stat 失败 | 404→dormant；网络错→退避；401/403→dormant 停 probe |
| 9 | ARMED 跨机新鲜度 | 60s 节流 Stat 和/或 SSE 接 index |
| 10 | dormantConfirmed 后跨机新注册 | 需 remount（或同 LocalRoot 本地信号） |
| 11 | 默认行为交付 | 发现、index、CLI 写序、SSE 接线同一版本落地，避免「只休眠不写 index」破坏 remount |
| 12 | `/.drive9` 隐藏 | 无强制隐藏，与 packs 同级 |
| 13 | 存量 | 无 reindex 命令；写路径 / `--git-workspaces=on` / 服务端维护 |

---

## 8. 正确性约束

1. `dormantConfirmed && !localArmSignal` ⇒ 0× git-workspaces HTTP。  
2. `localArmSignal` 必须是目录级。  
3. index 仅存在性；runtime 仅 List/Get。  
4. index 路径租户绝对；按 RemoteRoot 过滤。  
5. CLI：index + local armed 在 success/hydrate 之前；失败 fail 整命令。  
6. FUSE 删除 workspace 必须更新 index。  
7. list/arm singleflight + 失败退避；force 不绕过退避门，用 sticky 补投。  
8. DORMANT 下 `.git` 不得 force list。  
9. `/.drive9/` 无强制隐藏。

---

## 9. 实现锚点

| 区域 | 位置 |
| --- | --- |
| 状态机 / arm / list | `pkg/fuse/git_workspace.go` |
| 本地 armed 信号 | `pkg/gitcache/arm.go`（及既有 refresh/deleted marker） |
| 远程 index | `pkg/client/git_workspace_index.go` |
| CLI 写序 | `cmd/drive9/cli/git.go` |
| Mount probe | `pkg/fuse/mount.go`（SSE 启动后异步） |
| SSE index | `pkg/fuse/sse.go` |
| FUSE 删 workspace | `pkg/fuse` 删除 workspace 根路径 |
| CAS | `WriteCtxConditionalWithRevision` |
| e2e | `e2e/git-workspace-smoke-test.sh`（含 sandbox remount） |

---

## 10. 验收

1. 无 index、无本地 marker：mount + 活跃 FS 120s → `ListGitWorkspaces` = **0**，index Stat **≤1**（404）。  
2. 同 LocalRoot：`--fast` 后下一次 `ls`/stat 进入 git 层。  
3. 已 armed 时再注册新 workspace id（本地 marker mtime 前进）→ 会再 list。  
4. `sandbox_restore`（fresh LocalRoot remount）默认路径通过。  
5. 删空 + index 空后新 mount → 0 list。  
6. 网络故障下 list 次数受退避约束，非 op 级风暴。  
7. 既有 git-ops / git-workspace e2e 通过。

---

## 11. 流量量级（参考）

| 场景（约 10min 活跃 mount） | 改前 | 本方案 |
| --- | --- | --- |
| 从未 `--fast`，index 404 | ~1 list/s | **0** list；**1×** FS Stat |
| 同机中途 `--fast` | 持续 list | 武装后按需 |
| 新沙箱已有 workspace | 持续 list | 1× Stat + 1× list + 按需 |
| ARMED 跨机变更 | ~1s 收敛 | ≤60s（Stat）或近实时（SSE） |

---

## 12. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| 只上休眠、不上 index 写 → remount 丢发现 | 同一版本交付 DORMANT + index + CLI 写序 |
| index 与 DB 漂移 | list 为准；服务端维护 index；`--git-workspaces=on` 救急 |
| 并发写 index | revision CAS + 有界重试 |
| fs_scoped 读不到 `/.drive9/` | 文档说明；403 停 probe；`--git-workspaces=on` |
| 伪造 index | 武装后以 List 校验；index 不含敏感全量字段 |
| dormantConfirmed 后跨机新 `--fast` | 产品边界：remount；文档写清 |

---

## 13. 后续可选增强

- 武装后同 `HeadCommit` 跳过 `ListGitTree`（降本）。  
- 服务端 Upsert/Delete 自动维护 index（裸 API 一致，CLI CAS 可简化）。  
- CLI flag 矩阵：`--git-workspaces=auto|on|off|poll` 与 perf 指标（`arm_local` / `arm_index` / `list` / `index_stat`）。  
- fs_scoped 是否支持 RemoteRoot 下镜像 index 路径（若产品需要）。

以上增强不阻塞本方案默认路径。

---

## 14. 开放项（非阻塞）

1. fs_scoped 长期是否要求 scope 包含 `/.drive9/`，或提供 RemoteRoot 镜像路径。  
2. 服务端自动维护 index 的排期。  
3. 是否保留调试用 `poll` 模式（默认仍为 auto/按需）。
