/** Simplified Chinese locale. */
import type { LocaleKeys } from "./en";

const zhCN: Record<LocaleKeys, string> = {
  // Settings
  "settings.title": "drive9 设置",
  "settings.serverUrl": "服务器地址",
  "settings.serverUrl.desc": "drive9 服务器地址（例如 https://api.drive9.ai）",
  "settings.apiKey": "API 密钥",
  "settings.apiKey.desc": "drive9 身份验证密钥",
  "settings.testConnection": "测试连接",
  "settings.testConnection.desc": "验证服务器地址和 API 密钥",
  "settings.testConnection.btn": "测试",
  "settings.pushDebounce": "推送延迟（毫秒）",
  "settings.pushDebounce.desc": "文件修改后延迟同步的时间（默认：2000）",
  "settings.ignorePaths": "忽略路径",
  "settings.ignorePaths.desc": "排除同步的 glob 模式（每行一个）",
  "settings.maxFileSize": "最大文件大小（MB）",
  "settings.maxFileSize.desc": "跳过超过此大小的文件（默认：100）",
  "settings.mobileMaxFileSize": "移动端最大文件大小（MB）",
  "settings.mobileMaxFileSize.desc": "移动端文件大小上限，避免内存不足（默认：20）",
  "settings.enterServerUrl": "请先输入服务器地址",
  "settings.enterApiKey": "请先输入 API 密钥",
  "settings.connectionSuccess": "drive9：连接成功！",
  "settings.connectionFailed": "drive9：连接失败 — {error}",
  "settings.securityWarning": "⚠ 安全警告：",
  "settings.gitignoreNoFile": "未找到 .gitignore 文件。.obsidian/ 中的 API 密钥可能会被提交到 git。",
  "settings.gitignoreNoCoverage": ".gitignore 未覆盖 .obsidian/ — API 密钥可能会被提交到 git。",

  // Status bar
  "status.configure": "⚙ drive9：请在设置中配置",
  "status.reconciling": "↕ drive9：正在对账...",
  "status.synced": "✓ drive9：已同步",
  "status.syncedSkipped": "✓ drive9：已同步（{count} 个文件因过大被跳过）",
  "status.syncing": "↕ drive9：正在同步 {count} 个文件",
  "status.syncingProgress": "↕ drive9：{progress}",
  "status.offline": "⏸ drive9：离线",
  "status.error": "✗ drive9：错误",
  "status.errorDetail": "✗ drive9：{detail} 同步失败",
  "status.queued": "↕ drive9：已排队 {count} 个文件",
  "status.conflicts": "⚠ drive9：{count} 个冲突",
  "status.conflictsPlural": "⚠ drive9：{count} 个冲突",
  "status.firstRunFailed": "✗ drive9：首次运行失败",
  "status.queuing": "↕ drive9：排队中 {current}/{total} 个文件",

  // Commands
  "cmd.search": "搜索（drive9）",
  "cmd.retrySync": "重试失败的同步（drive9）",
  "cmd.searchRibbon": "搜索 drive9",

  // Notices — main
  "notice.configureFirst": "drive9：请先在设置中配置服务器地址",
  "notice.retrying": "drive9：正在重试同步...",
  "notice.firstRunFailed": "drive9：首次运行失败 — {error}",
  "notice.uploading": "drive9：正在上传 {count} 个文件到 drive9...",
  "notice.downloading": "drive9：正在从 drive9 下载...",
  "notice.downloaded": "drive9：已下载 {count} 个文件",
  "notice.firstRunCancelled": "drive9：首次运行已取消。同步已禁用。",

  // Notices — sync engine
  "notice.skippedLarge": "drive9：已跳过 {path}（{sizeMB} MB 超过 {limitMB} MB 限制）",
  "notice.conflictDetected": "drive9：检测到 {path} 的冲突",

  // Notices — conflict resolver
  "notice.autoMerged": "drive9：已自动合并 {path}",
  "notice.keptLocal": "drive9：已保留 {path} 的本地版本",
  "notice.keptRemote": "drive9：已保留 {path} 的远程版本",
  "notice.keptBoth": "drive9：已保留 {path} 的两个版本",
  "notice.remoteChangedRetry": "drive9：{path} 的远程内容再次变更，下个周期重试",

  // Search modal
  "search.placeholder": "在 drive9 中搜索文件...",
  "search.minChars": "请输入至少 3 个字符以搜索",
  "search.searching": "搜索中...",
  "search.noResults": "未找到结果",
  "search.notFoundLocally": "drive9：本地未找到文件 — {path}",

  // Conflict modal
  "conflict.title": "同步冲突",
  "conflict.local": "本地",
  "conflict.remote": "远程",
  "conflict.size": "大小",
  "conflict.modified": "修改时间",
  "conflict.diffPreview": "差异预览",
  "conflict.keepLocal": "保留本地",
  "conflict.keepRemote": "保留远程",
  "conflict.keepBoth": "保留两者",

  // Sync panel modal
  "syncPanel.title": "drive9 同步状态",
  "syncPanel.offline": "⏸ 离线 — 无法连接服务器",
  "syncPanel.error": "✗ 错误：同步失败",
  "syncPanel.errorDetail": "✗ 错误：{detail} 同步失败",
  "syncPanel.syncing": "↕ 正在同步 {count} 个文件...",
  "syncPanel.syncingPlural": "↕ 正在同步 {count} 个文件...",
  "syncPanel.allSynced": "✓ 所有文件已同步",
  "syncPanel.retryBtn": "重试同步",
  "syncPanel.conflictsTitle": "{count} 个冲突",
  "syncPanel.conflictsTitlePlural": "{count} 个冲突",
  "syncPanel.pendingFiles": "{count} 个文件等待同步",
  "syncPanel.pendingFilesPlural": "{count} 个文件等待同步",
  "syncPanel.skippedTitle": "{count} 个文件被跳过（过大）",

  // First run
  "firstRun.downloadTitle": "从 drive9 下载？",
  "firstRun.downloadMsg": "drive9 上有 {count} 个文件。是否下载到你的仓库？",
  "firstRun.yes": "是",
  "firstRun.cancel": "取消",
  "firstRun.bothSidesNotice": "发现 {count} 个文件在两端都存在。\n\n{files}\n\n无法在没有校验和的情况下验证内容一致性。\n这些文件将被标记为冲突，直到手动解决。\n仅存在于一端的文件将正常同步。",
  "firstRun.andMore": "...以及其他 {count} 个文件",
};

export default zhCN;
