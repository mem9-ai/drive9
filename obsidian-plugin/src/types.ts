/** Progress callback for multipart uploads. */
export type ProgressFn = (partNumber: number, totalParts: number) => void;

/** Plugin settings persisted in data.json. */
export interface Drive9Settings {
  serverUrl: string;
  apiKey: string;
  pushDebounce: number;
  ignorePaths: string[];
  maxFileSize: number; // bytes
  mobileMaxFileSize: number; // bytes — lower limit on mobile to avoid OOM
}

export const DEFAULT_SETTINGS: Drive9Settings = {
  serverUrl: "",
  apiKey: "",
  pushDebounce: 2000,
  ignorePaths: [".obsidian/**", ".trash/**", "*.tmp", ".DS_Store"],
  maxFileSize: 100 * 1024 * 1024, // 100MB
  mobileMaxFileSize: 20 * 1024 * 1024, // 20MB — lower limit on mobile to avoid OOM
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

/** Search result from GET /v1/fs/{path}?grep=... */
export interface SearchResult {
  path: string;
  name: string;
  size_bytes: number;
  score?: number;
}

/** Remote filesystem mutation event from /v1/events. */
export interface ChangeEvent {
  seq: number;
  path: string;
  op: string;
  actor?: string;
  ts: number;
}

/** Reset event from /v1/events when targeted replay is not possible. */
export interface ResetEvent {
  seq: number;
  reason: string;
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
  /** SHA-256 hex of content at last successful sync (for 3-way merge base). */
  lastSyncedContentHash?: string;
  /**
   * How a remote_deleted status was detected.
   * 'polling' requires 2 consecutive absences before apply; 'sse' is immediate.
   */
  deleteDetectionSource?: "polling" | "sse";
  /** Number of consecutive polls where this file was absent from remote. */
  consecutiveAbsences?: number;
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
