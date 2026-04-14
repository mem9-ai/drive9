/** English locale — the fallback language. */
const en = {
  // Settings
  "settings.title": "drive9 Settings",
  "settings.serverUrl": "Server URL",
  "settings.serverUrl.desc": "drive9 server address (e.g. https://api.drive9.ai)",
  "settings.apiKey": "API Key",
  "settings.apiKey.desc": "drive9 API key for authentication",
  "settings.testConnection": "Test Connection",
  "settings.testConnection.desc": "Verify server URL and API key",
  "settings.testConnection.btn": "Test",
  "settings.pushDebounce": "Push Debounce (ms)",
  "settings.pushDebounce.desc": "Delay before syncing after a file change (default: 2000)",
  "settings.ignorePaths": "Ignore Paths",
  "settings.ignorePaths.desc": "Glob patterns to exclude from sync (one per line)",
  "settings.maxFileSize": "Max File Size (MB)",
  "settings.maxFileSize.desc": "Skip files larger than this (default: 100)",
  "settings.mobileMaxFileSize": "Mobile Max File Size (MB)",
  "settings.mobileMaxFileSize.desc": "Lower file size limit on mobile to avoid OOM (default: 20)",
  "settings.enterServerUrl": "Please enter a server URL first",
  "settings.enterApiKey": "Please enter an API key first",
  "settings.connectionSuccess": "drive9: connection successful!",
  "settings.connectionFailed": "drive9: connection failed — {error}",
  "settings.securityWarning": "⚠ Security Warning: ",
  "settings.gitignoreNoFile": "No .gitignore found. Your API key in .obsidian/ could be committed to git.",
  "settings.gitignoreNoCoverage": ".gitignore does not cover .obsidian/ — your API key could be committed to git.",

  // Status bar
  "status.configure": "⚙ drive9: configure in settings",
  "status.reconciling": "↕ drive9: reconciling...",
  "status.synced": "✓ drive9: synced",
  "status.syncedSkipped": "✓ drive9: synced ({count} skipped — too large)",
  "status.syncing": "↕ drive9: syncing {count} files",
  "status.syncingProgress": "↕ drive9: {progress}",
  "status.offline": "⏸ drive9: offline",
  "status.error": "✗ drive9: error",
  "status.errorDetail": "✗ drive9: {detail} failed",
  "status.queued": "↕ drive9: queued {count} files",
  "status.conflicts": "⚠ drive9: {count} conflict",
  "status.conflictsPlural": "⚠ drive9: {count} conflicts",
  "status.firstRunFailed": "✗ drive9: first-run failed",
  "status.queuing": "↕ drive9: queuing {current}/{total} files",

  // Commands
  "cmd.search": "Search (drive9)",
  "cmd.retrySync": "Retry failed sync (drive9)",
  "cmd.searchRibbon": "Search drive9",

  // Notices — main
  "notice.configureFirst": "drive9: configure server URL in settings first",
  "notice.retrying": "drive9: retrying sync...",
  "notice.firstRunFailed": "drive9: first-run failed — {error}",
  "notice.uploading": "drive9: uploading {count} files to drive9...",
  "notice.downloading": "drive9: downloading from drive9...",
  "notice.downloaded": "drive9: downloaded {count} files",
  "notice.firstRunCancelled": "drive9: first-run cancelled. Sync is disabled.",

  // Notices — sync engine
  "notice.skippedLarge": "drive9: skipped {path} ({sizeMB} MB exceeds {limitMB} MB limit)",
  "notice.conflictDetected": "drive9: conflict detected for {path}",

  // Notices — conflict resolver
  "notice.autoMerged": "drive9: auto-merged {path}",
  "notice.keptLocal": "drive9: kept local version of {path}",
  "notice.keptRemote": "drive9: kept remote version of {path}",
  "notice.keptBoth": "drive9: kept both versions of {path}",
  "notice.remoteChangedRetry": "drive9: remote changed again for {path}, retrying next cycle",

  // Search modal
  "search.placeholder": "Search files in drive9...",
  "search.minChars": "Type at least 3 characters to search",
  "search.searching": "Searching...",
  "search.noResults": "No results found",
  "search.navigate": "navigate",
  "search.open": "open file",
  "search.dismiss": "dismiss",
  "search.error": "drive9 search: {detail}",
  "search.score": "score: {score}",
  "search.notFoundLocally": "drive9: file not found locally — {path}",

  // Conflict modal
  "conflict.title": "Sync Conflict",
  "conflict.local": "Local",
  "conflict.remote": "Remote",
  "conflict.size": "Size",
  "conflict.modified": "Modified",
  "conflict.diffPreview": "Diff preview",
  "conflict.keepLocal": "Keep Local",
  "conflict.keepRemote": "Keep Remote",
  "conflict.keepBoth": "Keep Both",

  // Sync panel modal
  "syncPanel.title": "drive9 Sync Status",
  "syncPanel.offline": "⏸ Offline — server unreachable",
  "syncPanel.error": "✗ Error: sync failed",
  "syncPanel.errorDetail": "✗ Error: {detail} failed to sync",
  "syncPanel.syncing": "↕ Syncing {count} file...",
  "syncPanel.syncingPlural": "↕ Syncing {count} files...",
  "syncPanel.allSynced": "✓ All files synced",
  "syncPanel.retryBtn": "Retry Sync",
  "syncPanel.conflictsTitle": "{count} Conflict",
  "syncPanel.conflictsTitlePlural": "{count} Conflicts",
  "syncPanel.pendingFiles": "{count} file queued for sync",
  "syncPanel.pendingFilesPlural": "{count} files queued for sync",
  "syncPanel.skippedTitle": "{count} Skipped (too large)",

  // First run
  "firstRun.downloadTitle": "Download from drive9?",
  "firstRun.downloadMsg": "drive9 has {count} files. Download them to your vault?",
  "firstRun.yes": "Yes",
  "firstRun.cancel": "Cancel",
  "firstRun.bothSidesNotice": "Found {count} file(s) that exist on both sides.\n\n{files}\n\nWithout checksums, content equality cannot be verified.\nThese files are marked as conflicts until manually resolved.\nFiles unique to each side will be synced normally.",
  "firstRun.andMore": "...and {count} more",
} as const;

export type LocaleKeys = keyof typeof en;
export default en;
