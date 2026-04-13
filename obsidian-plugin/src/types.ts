/** Plugin settings persisted in data.json. */
export interface Drive9Settings {
  serverUrl: string;
  apiKey: string;
  pushDebounce: number;
  ignorePaths: string[];
  maxFileSize: number; // bytes
}

export const DEFAULT_SETTINGS: Drive9Settings = {
  serverUrl: "",
  apiKey: "",
  pushDebounce: 2000,
  ignorePaths: [".obsidian/**", ".trash/**", "*.tmp", ".DS_Store"],
  maxFileSize: 100 * 1024 * 1024, // 100MB
};

/** Result of HEAD /v1/fs/{path} */
export interface StatResult {
  size: number;
  isDir: boolean;
  revision: number;
  mtime: number;
}

/** Entry from GET /v1/fs/{path}?list=1 */
export interface FileInfo {
  name: string;
  size: number;
  isDir: boolean;
  mtime: number;
}

/** Per-file sync tracking. */
export interface SyncState {
  path: string;
  localMtime: number;
  localSize: number;
  /**
   * null means "remote exists but revision is unknown" (e.g. stat failed).
   * 0 means "create-if-absent" (file not yet on remote).
   * Positive number is a real CAS revision.
   */
  remoteRevision: number | null;
  syncedAt: number;
  status: "synced" | "local_dirty" | "remote_dirty" | "remote_deleted" | "conflict" | "needs_refresh";
}

/** Persisted plugin data (settings + sync state). */
export interface PluginData {
  settings: Drive9Settings;
  syncStates: Record<string, SyncState>;
  firstRunComplete: boolean;
  actorId: string;
}

export const DEFAULT_PLUGIN_DATA: PluginData = {
  settings: DEFAULT_SETTINGS,
  syncStates: {},
  firstRunComplete: false,
  actorId: "",
};
