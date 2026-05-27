# Drive9 Git Fast Workspace 设计

## 背景

Drive9 的 agent 场景里有两个高优先级目标：

- `git clone` 到 mounted 目录必须快，不能把完整 working tree 内容全部写进 Drive9 通用文件表。
- sandbox/container 更换后，agent 上一次改到一半的 working tree 和 `.git` 状态必须能从 Drive9 恢复，继续 `git status`、`git add`、`git commit`、`git push`。

因此这不是一个纯通用 FS 优化，而是 Drive9 在通用 FUSE 语义之上增加的 Git-aware fast workspace。它刻意复用 Git 自己的对象模型：clean tree 用 Git manifest 表达，dirty working tree 用 Drive9 overlay 表达，`.git` 用本地 overlay 加后端 checkpoint 表达。

## 目标

- `drive9 git clone --fast <repo-url> <mounted-path>` 只注册 HEAD tree manifest，不把 clean file content checkout 到 `file_nodes`。
- FUSE 目录下看到完整文件树；clean file 读时按需从本地 blobless `.git` 取内容。
- edit/create/delete/chmod/symlink 等 working tree 变化持久化到 Drive9 后端 overlay。
- `.git` 保持本地磁盘读写性能，同时 checkpoint 到 Drive9，支持跨 sandbox 恢复。
- `git add`、`git commit`、`git push` 尽量走原生 Git 工作流，不引入额外 push API。

## 非目标

- 不做一整套 artifact 子命令，也不把方案扩展成通用 content-addressed checkout 工具。
- 不把 clean tree 文件内容写入 `file_nodes` / `contents`。
- 不在本地引入 SQLite。当前本地状态仍使用已有 local overlay、shadow、`journal.wal`；权威状态在 Drive9 后端 DB 的 git workspace 表。
- 不在初版处理复杂 Git 功能的完整语义，例如 submodule/LFS 深度优化、merge conflict 辅助、远端分支自动同步。

## 数据模型

`git_workspaces`

- 一行表示一个 fast workspace。
- 关键字段：`workspace_id`、`root_path`、`repo_url`、`remote_name`、`branch_name`、`base_commit`、`head_commit`、`mode=fast`、`status=active`。
- `root_path` 唯一，对应 mounted 目录里的 repo 根目录。

`git_workspace_tree_nodes`

- 保存 base/head commit 的 tree manifest。
- 路径相对 workspace root，不带前导 `/`。
- 包含目录、文件、symlink、submodule 的 `kind`、`mode`、`object_sha`、`size_bytes`。
- clean tree 的文件内容不在 Drive9 文件内容表里。

`git_workspace_overlay`

- 保存 working tree 相对 clean tree 的改动。
- `op=upsert` 表示新增或修改，`op=whiteout` 表示删除 clean tree 文件。
- 文件 payload 当前以内联 `content_blob` 形式存储；后续大文件可以扩展到 S3/storage ref。
- 目录创建也记录为 `kind=dir` 的 overlay entry。

`git_workspace_git_state`

- 保存 `.git` 目录 checkpoint。
- 当前格式是 `storage_type=tar.gz`，payload 存在 `content_blob`。
- sandbox B 挂载同一块盘时，FUSE 从这里恢复本地 `.git`。

`file_nodes`

- 仍只表达普通 Drive9 文件系统目录/文件。
- fast workspace 下的 clean/dirty Git 文件内容不应泄漏为 `file_nodes` 文件行。
- repo 父目录和 repo 根目录可以是普通目录行。

## Clone 流程

1. 用户先挂载 Drive9：

   ```bash
   drive9 mount --mode=fuse --profile=coding-agent --local-root <local-root> --cache-dir <cache> --durability=write-sync :/ <mountpoint>
   ```

2. 用户执行：

   ```bash
   drive9 git clone --fast https://github.com/org/repo.git <mountpoint>/<path>/repo
   ```

3. CLI 在 mounted path 下运行 blobless no-checkout clone：

   ```bash
   git clone --filter=blob:none --no-checkout <repo-url> <target>
   ```

   `.git` 被 coding-agent local overlay 路由到本地盘，不进入通用 Drive9 文件表。

4. CLI 读取 `HEAD`、branch、`git ls-tree -r -t -z HEAD`，生成 tree manifest。

   这里不能使用 `git ls-tree -l`。`-l` 会要求 Git 输出 blob size，而 blobless partial clone 下很多 blob 本地不存在，Git 会触发 lazy fetch，导致 fast clone 在 manifest 阶段退化成大量远端对象查询。

   对 GitHub repo，CLI 再调用 GitHub Trees API 获取 blob size 并补齐 `size_bytes`。这样 FUSE 可以给 `stat`/`git status` 暴露准确大小，但 clone 阶段仍不下载 blob 内容。非 GitHub 或 GitHub API 不可用时，`size_bytes=-1` 作为 unknown-size fallback，FUSE 会在真正读写该文件时按需读取 blob 并修正本地 inode size。

5. CLI 执行：

   ```bash
   git read-tree --reset HEAD
   ```

   这会初始化 index，让 Git 知道 clean tree 的 object ids，但不 checkout 文件内容。

6. CLI 调用 Drive9 API：

   - upsert `git_workspaces`
   - replace `git_workspace_tree_nodes`
   - archive 本地 overlay 里的 `.git`，upsert `git_workspace_git_state`

7. FUSE 重新发现 workspace 后，目录展示来自 `git_workspace_tree_nodes` + `git_workspace_overlay` 的合成视图。

## Read/Edit/Add/Commit/Push 流程

读 clean 文件：

- FUSE 查 workspace manifest，构造虚拟 inode。
- 文件被 read 时，FUSE 通过本地 `.git` 中的 Git 对象按需返回 blob 内容。
- 文件内容不写入 `file_nodes`。

编辑文件：

- 写 tracked clean file 时，FUSE 把新内容写入 `git_workspace_overlay` 的 `upsert` entry。
- 新建目录/文件同样进入 overlay。
- 删除 clean file 时写入 `whiteout`。
- `write-sync` 下写返回前 overlay 已上传；agent 中途停机也能恢复半成品。

`git add`：

- Git 从 FUSE working tree 读取 clean+overlay 合成内容。
- `.git/index`、objects、logs 等写入本地 overlay。
- 对 `.git` 的写打开句柄在 flush/release/rename 等写路径 checkpoint 到 `git_workspace_git_state`。
- 读打开的 `.git` 句柄不会 checkpoint，避免 `git status` 这类读操作反复上传整包 `.git`。

`git commit`：

- Git 原生更新本地 `.git`。
- commit 后 working tree overlay 与 Git index 匹配时，`git status` 为 clean。
- 注意：初版不会自动把新 commit 的 tree 反写成新的 clean manifest，也不会自动清空 overlay；overlay 仍是相对原始 base tree 的 working tree 状态。

`git push`：

- Git 原生 push。
- 对 partial/blobless clone，如果目标 remote 没有 base history，Git 可能需要补全历史对象；测试中使用预先 mirror 的 bare remote 模拟“远端已有 base history”的真实增量 push。
- Drive9 不参与 push 协议。

## Sandbox 更换流程

1. sandbox A 挂载 Drive9，fast clone repo。
2. agent 修改文件、`git add`、`git commit`、`git push`，随后又留下未提交修改。
3. A unmount 或停机。
4. sandbox B 用新的本地目录和 cache 挂载同一块 Drive9。
5. B 访问 repo path 时，FUSE 从后端加载：

   - `git_workspaces`
   - `git_workspace_tree_nodes`
   - `git_workspace_overlay`
   - `git_workspace_git_state`

6. FUSE 将 `.git` checkpoint 解包到 B 的 local overlay。
7. B 看到的 working tree = clean tree manifest + durable overlay；`git status`、`git log`、文件内容都恢复到 A 停机前状态。

## 本地状态与 SQLite

当前实现没有引入 SQLite。

本地目录里与 fast workspace 相关的状态包括：

- `<local-root>/overlay/.../.git`：本地 `.git`。
- `<cache-dir>/<mount-id>/journal.wal`：已有 FUSE write/cache journal。
- shadow/cache 文件：已有 write-back/strict durability 机制使用。

权威、可跨 sandbox 恢复的状态在后端 DB git workspace 表中；本地文件只是当前 sandbox 的性能层和临时恢复目标。

## Dev E2E 验证

2026-05-27 在 dev 环境验证通过：

- endpoint: `http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com`
- repo: `https://github.com/githubtraining/hellogitworld.git`
- run_id: `fastclone-e2e-20260527222938`
- tenant_id: `b1dc5fa2-f166-431b-9672-754887571975`
- workspace_id: `55ab295b-e674-4b24-80da-e515d7bf2f38`

覆盖场景：

- A 挂载后 `drive9 git clone --fast`。
- 修改 `README.txt`，新增 `notes/drive9-note.txt`，删除 `fix.txt`。
- `git add`、`git commit`、push 到预先 mirror 的 bare remote。
- commit 后继续修改 `build.gradle`，保持未提交状态。
- A unmount，B 使用新的 local root/cache 挂载同一块 Drive9。
- B 恢复 `.git`，`git log -1` 为刚提交的 commit，`git status --porcelain=v1` 为 ` M build.gradle`。

后端表检查：

- `git_workspaces`: active fast workspace row 存在。
- `git_workspace_tree_nodes`: 26 rows，等于 base commit tree count。
- `git_workspace_git_state`: `storage_type=tar.gz`，`content_len=46078`。
- `git_workspace_overlay`: 包含
  - `README.txt` upsert file
  - `notes` upsert dir
  - `notes/drive9-note.txt` upsert file
  - `fix.txt` whiteout file
  - `build.gradle` upsert file
- `file_nodes`: workspace 下 file row count 为 0；只存在 run 目录和 repo 根目录两条 directory rows。
- 本地 sqlite 文件数为 0。

## 已知限制与后续优化

- `.git` checkpoint 仍是整包 tar.gz。后续应做 debounce/coalesce、增量 checkpoint，或只在 idle/unmount/写事务边界上传。
- `git commit` 后 overlay 仍以 base tree 为基准保留；后续可以提供显式 `drive9 git checkpoint` 或自动推进 manifest 的机制。
- 大 overlay 文件需要从 `content_blob` 扩展到 S3/storage ref。
- branch 切换、merge、rebase、conflict、submodule、LFS 需要更完整的语义验证。
- 可以把 fast clone E2E 固化为脚本，避免只靠手工命令验证。
