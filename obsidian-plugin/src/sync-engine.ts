import { Vault, TFile, TAbstractFile, Notice, Platform } from "obsidian";
import { Drive9Client, Drive9Error, sanitizeError } from "./client";
import { IgnoreMatcher } from "./ignore";
import { t } from "./i18n";
import type { ShadowStore } from "./shadow-store";
import type { SyncState, Drive9Settings } from "./types";

export type SyncStatus = "idle" | "syncing" | "error" | "offline";

const LOCAL_EVENT_SUPPRESS_WINDOW_MS = 1000;
const OFFLINE_THRESHOLD = 3; // consecutive network failures before going offline

/**
 * SyncEngine owns the local/remote sync state machine.
 * Phase 2A adds remote detection, safe pull, and non-destructive delete/conflict handling.
 */
export class SyncEngine {
  private dirtyPaths = new Set<string>();
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private ignoreMatcher: IgnoreMatcher;
  private suppressedPaths = new Map<string, number>();
  private operationQueue: Promise<void> = Promise.resolve();
  private _status: SyncStatus = "idle";
  private _pendingCount = 0;
  private _uploadProgressText = "";
  private _skippedLargeFiles: string[] = [];
  private _lastErrorDetail = "";
  private _consecutiveNetworkFailures = 0;
  private statusListeners: Array<() => void> = [];
  private _conflictNoticeCount = 0;
  private _conflictNoticeTimer: ReturnType<typeof setTimeout> | null = null;

  private shadowStore: ShadowStore | null = null;

  constructor(
    private vault: Vault,
    private client: Drive9Client,
    private syncStates: Record<string, SyncState>,
    private settings: Drive9Settings,
    private persistData: () => Promise<void>,
  ) {
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
  }

  setShadowStore(store: ShadowStore): void {
    this.shadowStore = store;
  }

  get status(): SyncStatus {
    return this._status;
  }

  get pendingCount(): number {
    return this._pendingCount;
  }

  get uploadProgressText(): string {
    return this._uploadProgressText;
  }

  get skippedLargeFiles(): string[] {
    return this._skippedLargeFiles;
  }

  get lastErrorDetail(): string {
    return this._lastErrorDetail;
  }

  onStatusChange(fn: () => void): void {
    this.statusListeners.push(fn);
  }

  retrySync(): void {
    if ((this._status === "error" || this._status === "offline") && this.dirtyPaths.size > 0) {
      this._lastErrorDetail = "";
      this._consecutiveNetworkFailures = 0;
      this.setStatus("idle", this.dirtyPaths.size);
      this.scheduleFlush();
    }
  }

  updateSettings(settings: Drive9Settings): void {
    this.settings = settings;
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
  }

  onLocalCreate(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.isLocalEventSuppressed(file.path)) return;
    if (this.matchesSyncedSnapshot(file.path, file)) return;
    this.markDirty(file.path);
  }

  onLocalModify(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.isLocalEventSuppressed(file.path)) return;
    if (this.matchesSyncedSnapshot(file.path, file)) return;
    this.markDirty(file.path);
  }

  onLocalDelete(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    if (this.isLocalEventSuppressed(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalRename(file: TAbstractFile, oldPath: string): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path) && this.shouldIgnore(oldPath)) return;
    if (!this.shouldIgnore(oldPath) && !this.isLocalEventSuppressed(oldPath)) {
      this.markDirty(oldPath);
    }
    if (
      !this.shouldIgnore(file.path) &&
      !this.isLocalEventSuppressed(file.path) &&
      !this.matchesSyncedSnapshot(file.path, file)
    ) {
      this.markDirty(file.path);
    }
  }

  async onRemoteChange(path: string, op: string): Promise<void> {
    if (this.shouldIgnore(path)) return;
    await this.enqueue(async () => {
      this.setStatus("syncing", 1);
      try {
        await this.reconcileRemotePath(path, op === "delete" ? "sse" : undefined);
      } finally {
        this.setStatus("idle", this.dirtyPaths.size);
        await this.persistData();
      }
    });
  }

  async fullSync(): Promise<void> {
    await this.enqueue(async () => {
      const scan = await this.client.listRecursiveDetailed("/");
      const remotePaths = new Set<string>();

      this.setStatus("syncing", scan.entries.length);
      try {
        for (const entry of scan.entries) {
          if (this.shouldIgnore(entry.name)) continue;
          remotePaths.add(entry.name);
          await this.reconcileRemotePath(entry.name);
        }

        if (scan.complete) {
          for (const path of Object.keys(this.syncStates)) {
            if (this.shouldIgnore(path) || remotePaths.has(path)) continue;
            await this.handleRemoteMissing(path);
          }
        } else {
          console.warn("[drive9] remote tree scan incomplete; skipping delete detection");
        }
      } finally {
        this.setStatus("idle", this.dirtyPaths.size);
        await this.persistData();
      }
    });
  }

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
      void this.flush();
    }, this.settings.pushDebounce);
  }

  private async flush(): Promise<void> {
    await this.enqueue(async () => {
      if (this.dirtyPaths.size === 0) return;

      const paths = [...this.dirtyPaths];
      this.dirtyPaths.clear();
      this._skippedLargeFiles = [];

      this.setStatus("syncing", paths.length);

      let errorOccurred = false;
      let lastFailedPath = "";
      let networkError = false;

      for (const path of paths) {
        try {
          await this.pushOne(path);
          this._consecutiveNetworkFailures = 0;
        } catch (e) {
          errorOccurred = true;
          lastFailedPath = path;
          if (this.isNetworkError(e)) {
            networkError = true;
          }
          console.error(`[drive9] push failed: ${path}`, e instanceof Error ? e.message : sanitizeError(String(e)));
          this.dirtyPaths.add(path);
        }
      }

      this._lastErrorDetail = lastFailedPath;
      if (networkError) {
        this._consecutiveNetworkFailures++;
      } else if (errorOccurred) {
        // Non-network error breaks the consecutive network failure streak
        this._consecutiveNetworkFailures = 0;
      }
      const finalStatus: SyncStatus = errorOccurred
        ? (this._consecutiveNetworkFailures >= OFFLINE_THRESHOLD ? "offline" : "error")
        : "idle";
      this.setStatus(finalStatus, this.dirtyPaths.size);
      await this.persistData();
    });
  }

  private async pushOne(path: string): Promise<void> {
    const file = this.vault.getAbstractFileByPath(path);

    if (!file || !(file instanceof TFile)) {
      const state = this.syncStates[path];
      if (state) {
        try {
          await this.client.delete(path);
        } catch (e) {
          if (!(e instanceof Drive9Error && e.status === 404)) {
            throw e;
          }
        }
        delete this.syncStates[path];
      }
      return;
    }

    const effectiveMaxSize = Platform.isMobile
      ? Math.min(this.settings.maxFileSize, this.settings.mobileMaxFileSize)
      : this.settings.maxFileSize;
    if (file.stat.size > effectiveMaxSize) {
      const sizeMB = (file.stat.size / (1024 * 1024)).toFixed(1);
      const limitMB = (effectiveMaxSize / (1024 * 1024)).toFixed(0);
      console.warn(`[drive9] skipping large file: ${path} (${file.stat.size} bytes, limit ${effectiveMaxSize})`);
      new Notice(t("notice.skippedLarge", { path, sizeMB, limitMB }));
      if (!this._skippedLargeFiles.includes(path)) {
        this._skippedLargeFiles.push(path);
      }
      this.notifyStatusChange();
      return;
    }

    const existingState = this.syncStates[path];
    if (existingState?.status === "conflict") {
      return;
    }

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
    const expectedRevision = existingState ? existingState.remoteRevision : 0;

    try {
      const result = await this.client.write(path, data, expectedRevision, (part, total) => {
        const fileName = path.includes("/") ? path.slice(path.lastIndexOf("/") + 1) : path;
        this._uploadProgressText = `${fileName} part ${part}/${total}`;
        this.notifyStatusChange();
      });
      this._uploadProgressText = "";
      const contentHash = await this.saveShadowIfAvailable(data);
      if (result.revision !== null) {
        this.syncStates[path] = {
          path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: result.revision,
          syncedAt: Date.now(),
          status: "synced",
          lastSyncedContentHash: contentHash,
        };
      } else {
        this.syncStates[path] = {
          path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: null,
          syncedAt: Date.now(),
          status: "needs_refresh",
          lastSyncedContentHash: contentHash,
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
        new Notice(t("notice.conflictDetected", { path }));
        return;
      }
      throw e;
    }
  }

  private enqueue(fn: () => Promise<void>): Promise<void> {
    const next = this.operationQueue.then(fn, fn);
    this.operationQueue = next.catch(() => undefined);
    return next;
  }

  private isLocalEventSuppressed(path: string): boolean {
    return (this.suppressedPaths.get(path) ?? 0) > 0;
  }

  async withSuppressedLocalEvents(path: string, fn: () => Promise<void>): Promise<void> {
    this.suppressedPaths.set(path, (this.suppressedPaths.get(path) ?? 0) + 1);
    try {
      await fn();
    } finally {
      setTimeout(() => {
        const current = this.suppressedPaths.get(path) ?? 0;
        if (current <= 1) {
          this.suppressedPaths.delete(path);
        } else {
          this.suppressedPaths.set(path, current - 1);
        }
      }, LOCAL_EVENT_SUPPRESS_WINDOW_MS);
    }
  }

  private getLocalFile(path: string): TFile | null {
    const file = this.vault.getAbstractFileByPath(path);
    return file instanceof TFile ? file : null;
  }

  private hasUnpushedLocalChange(path: string, file: TFile | null, state: SyncState | undefined): boolean {
    if (this.dirtyPaths.has(path)) return true;
    if (!file) {
      // Remote-only paths should pull instead of turning into conflicts.
      return false;
    }
    if (!state) return true;
    if (state.status === "local_dirty" || state.status === "conflict") return true;
    return file.stat.mtime !== state.localMtime || file.stat.size !== state.localSize;
  }

  private matchesSyncedSnapshot(path: string, file: TFile): boolean {
    const state = this.syncStates[path];
    if (!state || state.status !== "synced") {
      return false;
    }
    return state.localMtime === file.stat.mtime && state.localSize === file.stat.size;
  }

  private async reconcileRemotePath(path: string, deleteSource?: "polling" | "sse"): Promise<void> {
    const state = this.syncStates[path];
    if (state?.status === "conflict") {
      return;
    }

    const localFile = this.getLocalFile(path);

    let remoteRevision: number;
    try {
      const remoteStat = await this.client.stat(path);
      remoteRevision = remoteStat.revision;
    } catch (error) {
      if (error instanceof Drive9Error && error.status === 404) {
        await this.handleRemoteMissing(path, deleteSource ?? "polling");
        return;
      }
      throw error;
    }

    if (
      state?.status === "synced" &&
      state.remoteRevision === remoteRevision &&
      !this.hasUnpushedLocalChange(path, localFile, state)
    ) {
      return;
    }

    if (this.hasUnpushedLocalChange(path, localFile, state)) {
      this.markConflict(path, localFile, remoteRevision);
      return;
    }

    await this.pullRemoteFile(path, remoteRevision, localFile);
  }

  private async handleRemoteMissing(path: string, source: "polling" | "sse" = "polling"): Promise<void> {
    const state = this.syncStates[path];
    if (!state || state.status === "conflict") return;

    const localFile = this.getLocalFile(path);
    if (this.hasUnpushedLocalChange(path, localFile, state)) {
      this.markConflict(path, localFile, state.remoteRevision);
      return;
    }

    if (state.status === "remote_deleted") {
      // Already marked — increment consecutive absence count for polling
      if (source === "polling") {
        state.consecutiveAbsences = (state.consecutiveAbsences ?? 1) + 1;
      }
      return;
    }

    state.status = "remote_deleted";
    state.syncedAt = Date.now();
    state.deleteDetectionSource = source;
    state.consecutiveAbsences = source === "polling" ? 1 : undefined;
  }

  private markConflict(path: string, localFile: TFile | null, remoteRevision: number | null): void {
    const existingState = this.syncStates[path];
    const wasConflict = existingState?.status === "conflict";

    this.syncStates[path] = {
      path,
      localMtime: localFile?.stat.mtime ?? existingState?.localMtime ?? 0,
      localSize: localFile?.stat.size ?? existingState?.localSize ?? 0,
      remoteRevision,
      syncedAt: existingState?.syncedAt ?? 0,
      status: "conflict",
    };

    if (!wasConflict) {
      this.notifyConflict(path);
    }
  }

  /**
   * Batched conflict notification: show individual notices for the first few,
   * then a single summary notice for the rest.
   */
  private notifyConflict(path: string): void {
    this._conflictNoticeCount++;

    // Show individual notices only for the first 3 conflicts in a batch
    if (this._conflictNoticeCount <= 3) {
      new Notice(t("notice.conflictDetected", { path }));
    }

    // Reset batch window after 5 seconds of no new conflicts
    if (this._conflictNoticeTimer) clearTimeout(this._conflictNoticeTimer);
    this._conflictNoticeTimer = setTimeout(() => {
      if (this._conflictNoticeCount > 3) {
        const total = this.countConflicts();
        new Notice(t("notice.conflictsBatch", { count: total }), 10000);
      }
      this._conflictNoticeCount = 0;
      this._conflictNoticeTimer = null;
    }, 5000);
  }

  private countConflicts(): number {
    let count = 0;
    for (const state of Object.values(this.syncStates)) {
      if (state.status === "conflict") count++;
    }
    return count;
  }

  private async pullRemoteFile(path: string, remoteRevision: number, localFile: TFile | null): Promise<void> {
    const data = await this.client.read(path);

    await this.withSuppressedLocalEvents(path, async () => {
      const dir = path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "";
      if (dir && !this.vault.getAbstractFileByPath(dir)) {
        await this.vault.createFolder(dir);
      }
      if (localFile) {
        await this.vault.modifyBinary(localFile, data);
      } else {
        await this.vault.createBinary(path, data);
      }
    });

    const contentHash = await this.saveShadowIfAvailable(data);
    const updatedFile = this.getLocalFile(path);
    this.syncStates[path] = {
      path,
      localMtime: updatedFile?.stat.mtime ?? 0,
      localSize: updatedFile?.stat.size ?? 0,
      remoteRevision,
      syncedAt: Date.now(),
      status: "synced",
      lastSyncedContentHash: contentHash,
    };
  }

  private async saveShadowIfAvailable(data: ArrayBuffer): Promise<string | undefined> {
    if (!this.shadowStore) return undefined;
    try {
      return await this.shadowStore.save(data);
    } catch {
      return undefined;
    }
  }

  private setStatus(status: SyncStatus, pending: number): void {
    this._status = status;
    this._pendingCount = pending;
    this.notifyStatusChange();
  }

  private notifyStatusChange(): void {
    for (const fn of this.statusListeners) {
      try {
        fn();
      } catch {
        // Ignore status listener failures.
      }
    }
  }

  private isNetworkError(e: unknown): boolean {
    if (e instanceof Drive9Error) {
      // 0 = no response (network), 502/503/504 = server unreachable
      return e.status === 0 || e.status >= 502;
    }
    if (e instanceof TypeError) {
      // fetch() throws TypeError on network failure
      return true;
    }
    const msg = e instanceof Error ? e.message : "";
    return /network|fetch|ECONNREFUSED|ENOTFOUND|ETIMEDOUT/i.test(msg);
  }
}
