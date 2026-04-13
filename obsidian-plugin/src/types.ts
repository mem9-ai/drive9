export interface Drive9PluginSettings {
  serverUrl: string;
  apiKey: string;
  pushDebounce: number;
  ignorePaths: string[];
  maxFileSize: number;
}

export const DEFAULT_SETTINGS: Drive9PluginSettings = {
  serverUrl: "",
  apiKey: "",
  pushDebounce: 2000,
  ignorePaths: [".obsidian/**", ".trash/**", "*.tmp", ".DS_Store"],
  maxFileSize: 100 * 1024 * 1024, // 100MB
};

export interface FileInfo {
  name: string;
  size: number;
  isDir: boolean;
  mtime: number;
}

export interface StatResult {
  size: number;
  isDir: boolean;
  revision: number;
  mtime: number;
}

export interface SyncStateEntry {
  path: string;
  localMtime: number;
  localSize: number;
  remoteRevision: number;
  syncedAt: number;
  status: "synced" | "local_dirty" | "conflict";
}

export interface ReconcileAction {
  path: string;
  action: "push" | "pull" | "conflict" | "skip";
  localSize?: number;
  remoteSize?: number;
}
