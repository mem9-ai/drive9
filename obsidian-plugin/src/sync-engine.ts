import { Vault, TFile, TAbstractFile, Notice } from "obsidian";
import { Drive9Client, Drive9Error } from "./client";
import { IgnoreMatcher } from "./ignore";
import type { SyncState, Drive9Settings } from "./types";

export type SyncStatus = "idle" | "syncing" | "error";

/**
 * SyncEngine handles bidirectional sync between local vault and drive9.
 * Phase 1: local changes → debounce → push with CAS.
 * Phase 2A: remote changes → detect via SSE/polling → pull or mark conflict.
 */
export class SyncEngine {
  private dirtyPaths = new Set<string>();
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private ignoreMatcher: IgnoreMatcher;
  private _status: SyncStatus = "idle";
  private _pendingCount = 0;
  private statusListeners: Array<() => void> = [];
  private _actorId: string;
  /** Paths currently being written by pull — suppress vault events for these. */
  private pullingPaths = new Set<string>();
  private syncLock = false;

  constructor(
    private vault: Vault,
    private client: Drive9Client,
    private syncStates: Record<string, SyncState>,
    private settings: Drive9Settings,
    private persistData: () => Promise<void>,
    actorId: string,
  ) {
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
    this._actorId = actorId;
  }

  get status(): SyncStatus {
    return this._status;
  }

  get pendingCount(): number {
    return this._pendingCount;
  }

  get actorId(): string {
    return this._actorId;
  }

  onStatusChange(fn: () => void): void {
    this.statusListeners.push(fn);
  }

  updateSettings(settings: Drive9Settings): void {
    this.settings = settings;
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
  }

  // ---------------------------------------------------------------------------
  // Vault event handlers (local changes)
  // ---------------------------------------------------------------------------

  onLocalCreate(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.pullingPaths.has(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalModify(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.pullingPaths.has(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalDelete(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.pullingPaths.has(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalRename(file: TAbstractFile, oldPath: string): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path) && this.shouldIgnore(oldPath)) return;
    if (this.pullingPaths.has(file.path) || this.pullingPaths.has(oldPath)) return;
    if (!this.shouldIgnore(oldPath)) {
      this.markDirty(oldPath);
    }
    if (!this.shouldIgnore(file.path)) {
      this.markDirty(file.path);
    }
  }

  // ---------------------------------------------------------------------------
  // Remote change handlers (SSE / polling callbacks)
  // ---------------------------------------------------------------------------

  /** Called by SSEWatcher when a single remote file changed. */
  onRemoteChange(path: string, op: string): void {
    if (this.shouldIgnore(path)) return;

    const state = this.syncStates[path];

    if (op === "delete") {
      if (state) {
        // Mark remote_deleted — Phase 2B will apply the actual delete.
        state.status = "remote_deleted";
      }
      this.notifyStatusChange();
      this.persistData();
      return;
    }

    // write op — schedule a pull check.
    if (state) {
      if (state.status === "local_dirty" || state.status === "conflict") {
        // Local is also dirty → conflict.
        state.status = "conflict";
        this.notifyStatusChange();
        this.persistData();
        return;
      }
      state.status = "remote_dirty";
    } else {
      // New remote file — create a placeholder state to trigger pull.
      this.syncStates[path] = {
        path,
        localMtime: 0,
        localSize: 0,
        remoteRevision: null,
        syncedAt: 0,
        status: "remote_dirty",
      };
    }

    this.schedulePull();
  }

  /** Called by SSEWatcher on reset event or polling interval. */
  async onFullSync(): Promise<void> {
    if (this.syncLock) return;
    this.syncLock = true;
    try {
      await this.pollRemoteChanges();
    } catch (e) {
      console.error("[drive9] full sync failed", e);
    } finally {
      this.syncLock = false;
    }
  }

  // ---------------------------------------------------------------------------
  // Polling: diff remote state against local SyncState
  // ---------------------------------------------------------------------------

  private async pollRemoteChanges(): Promise<void> {
    const remoteFiles = await this.buildRemoteFileMap();

    let changed = false;

    // Check for remote deletions and modifications of tracked files.
    for (const [path, state] of Object.entries(this.syncStates)) {
      if (this.shouldIgnore(path)) continue;
      if (state.status === "conflict") continue; // don't touch conflicts

      if (!remoteFiles.has(path)) {
        if (state.status === "synced") {
          state.status = "remote_deleted";
          changed = true;
        }
        continue;
      }

      const remote = remoteFiles.get(path)!;
      if (remote.revision !== state.remoteRevision && state.status === "synced") {
        state.status = "remote_dirty";
        changed = true;
      } else if (remote.revision !== state.remoteRevision && state.status === "local_dirty") {
        state.status = "conflict";
        changed = true;
      }
    }

    // Check for new remote files not in SyncState.
    for (const [path, info] of remoteFiles) {
      if (this.shouldIgnore(path)) continue;
      if (this.syncStates[path]) continue;

      // Check if file exists locally already (e.g. created between polls).
      const localFile = this.vault.getAbstractFileByPath(path);
      if (localFile instanceof TFile) {
        // Both exist but no sync state — treat as conflict.
        this.syncStates[path] = {
          path,
          localMtime: localFile.stat.mtime,
          localSize: localFile.stat.size,
          remoteRevision: info.revision,
          syncedAt: 0,
          status: "conflict",
        };
      } else {
        // New remote file — mark for pull.
        this.syncStates[path] = {
          path,
          localMtime: 0,
          localSize: 0,
          remoteRevision: info.revision,
          syncedAt: 0,
          status: "remote_dirty",
        };
      }
      changed = true;
    }

    if (changed) {
      this.notifyStatusChange();
      await this.persistData();
      await this.pullDirtyFiles();
    }
  }

  /**
   * Build a map of all remote files with their revisions.
   * Uses listRecursive + stat for revision data.
   */
  private async buildRemoteFileMap(): Promise<Map<string, { revision: number }>> {
    const entries = await this.client.listRecursive("/");
    const result = new Map<string, { revision: number }>();

    for (const entry of entries) {
      if (this.shouldIgnore(entry.name)) continue;
      if (entry.isDir) continue;
      try {
        const st = await this.client.stat(entry.name);
        result.set(entry.name, { revision: st.revision });
      } catch {
        // stat failed — skip this file for this poll cycle.
      }
    }

    return result;
  }

  // ---------------------------------------------------------------------------
  // Pull logic: download remote changes to local vault
  // ---------------------------------------------------------------------------

  private pullTimer: ReturnType<typeof setTimeout> | null = null;

  private schedulePull(): void {
    if (this.pullTimer) return; // already scheduled
    this.pullTimer = setTimeout(() => {
      this.pullTimer = null;
      this.pullDirtyFiles();
    }, 500); // small delay to batch multiple SSE events
  }

  private async pullDirtyFiles(): Promise<void> {
    const toPull = Object.values(this.syncStates).filter(
      (s) => s.status === "remote_dirty",
    );

    if (toPull.length === 0) return;

    this.setStatus("syncing", toPull.length);

    let errorOccurred = false;

    for (const state of toPull) {
      try {
        await this.pullOne(state);
      } catch (e) {
        errorOccurred = true;
        console.error(`[drive9] pull failed: ${state.path}`, e);
      }
    }

    this.setStatus(errorOccurred ? "error" : "idle", 0);
    await this.persistData();
  }

  private async pullOne(state: SyncState): Promise<void> {
    const path = state.path;

    // Get the latest remote content.
    const data = await this.client.read(path);

    // Get the latest revision.
    let revision: number | null = state.remoteRevision;
    try {
      const st = await this.client.stat(path);
      revision = st.revision;
    } catch {
      // Keep whatever revision we had.
    }

    // Write to local vault, suppressing vault events.
    this.pullingPaths.add(path);
    try {
      const existing = this.vault.getAbstractFileByPath(path);
      if (existing instanceof TFile) {
        await this.vault.modifyBinary(existing, data);
      } else {
        // Ensure parent directory exists.
        const dir = path.contains("/")
          ? path.substring(0, path.lastIndexOf("/"))
          : "";
        if (dir && !this.vault.getAbstractFileByPath(dir)) {
          await this.vault.createFolder(dir);
        }
        await this.vault.createBinary(path, data);
      }

      // Update sync state.
      const file = this.vault.getAbstractFileByPath(path);
      if (file instanceof TFile) {
        state.localMtime = file.stat.mtime;
        state.localSize = file.stat.size;
      }
      state.remoteRevision = revision;
      state.syncedAt = Date.now();
      state.status = revision !== null ? "synced" : "needs_refresh";
    } finally {
      this.pullingPaths.delete(path);
    }
  }

  // ---------------------------------------------------------------------------
  // Push logic (from Phase 1, unchanged)
  // ---------------------------------------------------------------------------

  private shouldIgnore(path: string): boolean {
    return this.ignoreMatcher.isIgnored(path);
  }

  private markDirty(path: string): void {
    this.dirtyPaths.add(path);
    const state = this.syncStates[path];
    if (state && state.status !== "conflict") {
      state.status = "local_dirty";
    }
    this.scheduleFlush();
  }

  private scheduleFlush(): void {
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      this.flush();
    }, this.settings.pushDebounce);
  }

  private async flush(): Promise<void> {
    if (this.dirtyPaths.size === 0) return;

    const paths = [...this.dirtyPaths];
    this.dirtyPaths.clear();

    this.setStatus("syncing", paths.length);

    let errorOccurred = false;

    for (const path of paths) {
      try {
        await this.pushOne(path);
      } catch (e) {
        errorOccurred = true;
        console.error(`[drive9] push failed: ${path}`, e);
        this.dirtyPaths.add(path);
      }
    }

    this.setStatus(errorOccurred ? "error" : "idle", this.dirtyPaths.size);
    await this.persistData();
  }

  private async pushOne(path: string): Promise<void> {
    const file = this.vault.getAbstractFileByPath(path);

    // File was deleted locally.
    if (!file || !(file instanceof TFile)) {
      const state = this.syncStates[path];
      if (state) {
        try {
          await this.client.delete(path);
        } catch (e) {
          if (e instanceof Drive9Error && e.status === 404) {
            // ok
          } else {
            throw e;
          }
        }
        delete this.syncStates[path];
      }
      return;
    }

    // Skip files exceeding size limit.
    if (file.stat.size > this.settings.maxFileSize) {
      console.warn(`[drive9] skipping large file: ${path} (${file.stat.size} bytes)`);
      return;
    }

    // Skip files in conflict or remote_deleted state.
    const existingState = this.syncStates[path];
    if (existingState?.status === "conflict" || existingState?.status === "remote_deleted") {
      return;
    }

    // If revision is unknown (null), try to refresh before pushing.
    if (existingState && existingState.remoteRevision === null) {
      try {
        const st = await this.client.stat(path);
        existingState.remoteRevision = st.revision;
        existingState.status = "local_dirty";
      } catch {
        existingState.status = "needs_refresh";
        console.warn(`[drive9] cannot refresh revision for ${path}, blocking push`);
        return;
      }
    }

    const data = await this.vault.readBinary(file);
    // If no state exists, this is a new file — use 0 (create-if-absent).
    const expectedRevision = existingState ? existingState.remoteRevision : 0;

    try {
      const result = await this.client.write(path, data, expectedRevision);
      if (result.revision !== null) {
        this.syncStates[path] = {
          path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: result.revision,
          syncedAt: Date.now(),
          status: "synced",
        };
      } else {
        this.syncStates[path] = {
          path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: null,
          syncedAt: Date.now(),
          status: "needs_refresh",
        };
        console.warn(`[drive9] write succeeded but revision unknown for ${path}`);
      }
    } catch (e) {
      if (e instanceof Drive9Error && e.status === 409) {
        if (existingState) {
          existingState.status = "conflict";
        } else {
          this.syncStates[path] = {
            path,
            localMtime: file.stat.mtime,
            localSize: file.stat.size,
            remoteRevision: null,
            syncedAt: 0,
            status: "conflict",
          };
        }
        new Notice(`drive9: conflict detected for ${path}`);
        return;
      }
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Status
  // ---------------------------------------------------------------------------

  private setStatus(status: SyncStatus, pending: number): void {
    this._status = status;
    this._pendingCount = pending;
    this.notifyStatusChange();
  }

  private notifyStatusChange(): void {
    for (const fn of this.statusListeners) {
      try { fn(); } catch { /* ignore */ }
    }
  }
}
