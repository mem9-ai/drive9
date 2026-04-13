import { Vault, TFile, TAbstractFile, Notice } from "obsidian";
import { Drive9Client, Drive9Error } from "./client";
import { IgnoreMatcher } from "./ignore";
import type { SyncState, Drive9Settings } from "./types";

export type SyncStatus = "idle" | "syncing" | "error";

/**
 * SyncEngine handles one-way push from local vault to drive9.
 * Phase 1: local changes → debounce → push with CAS.
 */
export class SyncEngine {
  private dirtyPaths = new Set<string>();
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private ignoreMatcher: IgnoreMatcher;
  private _status: SyncStatus = "idle";
  private _pendingCount = 0;
  private statusListeners: Array<() => void> = [];

  constructor(
    private vault: Vault,
    private client: Drive9Client,
    private syncStates: Record<string, SyncState>,
    private settings: Drive9Settings,
    private persistData: () => Promise<void>,
  ) {
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
  }

  get status(): SyncStatus {
    return this._status;
  }

  get pendingCount(): number {
    return this._pendingCount;
  }

  onStatusChange(fn: () => void): void {
    this.statusListeners.push(fn);
  }

  updateSettings(settings: Drive9Settings): void {
    this.settings = settings;
    this.ignoreMatcher = new IgnoreMatcher(settings.ignorePaths);
  }

  // ---------------------------------------------------------------------------
  // Vault event handlers
  // ---------------------------------------------------------------------------

  onLocalCreate(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalModify(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalDelete(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path)) return;
    this.markDirty(file.path);
  }

  onLocalRename(file: TAbstractFile, oldPath: string): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldIgnore(file.path) && this.shouldIgnore(oldPath)) return;
    // Treat rename as delete old + create new.
    if (!this.shouldIgnore(oldPath)) {
      this.markDirty(oldPath);
    }
    if (!this.shouldIgnore(file.path)) {
      this.markDirty(file.path);
    }
  }

  // ---------------------------------------------------------------------------
  // Core sync logic
  // ---------------------------------------------------------------------------

  private shouldIgnore(path: string): boolean {
    return this.ignoreMatcher.isIgnored(path);
  }

  private markDirty(path: string): void {
    this.dirtyPaths.add(path);
    const state = this.syncStates[path];
    if (state) {
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
        // Re-add to dirty set for retry on next flush.
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
          // 404 is fine — already gone on remote.
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

    const data = await this.vault.readBinary(file);
    const state = this.syncStates[path];
    const expectedRevision = state?.remoteRevision;

    try {
      const result = await this.client.write(path, data, expectedRevision);
      this.syncStates[path] = {
        path,
        localMtime: file.stat.mtime,
        localSize: file.stat.size,
        remoteRevision: result.revision,
        syncedAt: Date.now(),
        status: "synced",
      };
    } catch (e) {
      if (e instanceof Drive9Error && e.status === 409) {
        // CAS conflict — mark but don't overwrite.
        if (state) {
          state.status = "conflict";
        } else {
          this.syncStates[path] = {
            path,
            localMtime: file.stat.mtime,
            localSize: file.stat.size,
            remoteRevision: 0,
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

  private setStatus(status: SyncStatus, pending: number): void {
    this._status = status;
    this._pendingCount = pending;
    for (const fn of this.statusListeners) {
      try { fn(); } catch { /* ignore */ }
    }
  }
}
