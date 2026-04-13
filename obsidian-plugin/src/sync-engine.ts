import { Notice, TFile, TAbstractFile, Vault } from "obsidian";
import { Drive9Client, RevisionConflictError } from "./client";
import type { SyncStateEntry } from "./types";
import { shouldIgnore } from "./ignore";

export type SyncStatus = "idle" | "syncing" | "error";
type StatusListener = (status: SyncStatus, detail?: string) => void;

export class SyncEngine {
  private client: Drive9Client;
  private vault: Vault;
  private syncState: Map<string, SyncStateEntry>;
  private dirtyPaths = new Set<string>();
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private debounceMs: number;
  private ignorePaths: string[];
  private maxFileSize: number;
  private running = false;
  private statusListener: StatusListener | null = null;

  constructor(
    client: Drive9Client,
    vault: Vault,
    syncState: Map<string, SyncStateEntry>,
    opts: {
      debounceMs: number;
      ignorePaths: string[];
      maxFileSize: number;
    },
  ) {
    this.client = client;
    this.vault = vault;
    this.syncState = syncState;
    this.debounceMs = opts.debounceMs;
    this.ignorePaths = opts.ignorePaths;
    this.maxFileSize = opts.maxFileSize;
  }

  onStatus(listener: StatusListener): void {
    this.statusListener = listener;
  }

  private emitStatus(status: SyncStatus, detail?: string): void {
    this.statusListener?.(status, detail);
  }

  getSyncState(): Map<string, SyncStateEntry> {
    return this.syncState;
  }

  // --- Vault event handlers ---

  onFileCreate(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldSkip(file)) return;
    this.markDirty(file.path);
  }

  onFileModify(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (this.shouldSkip(file)) return;
    this.markDirty(file.path);
  }

  onFileDelete(file: TAbstractFile): void {
    if (!(file instanceof TFile)) return;
    if (shouldIgnore(file.path, this.ignorePaths)) return;
    this.dirtyPaths.add("DELETE:" + file.path);
    this.schedulePush();
  }

  onFileRename(file: TAbstractFile, oldPath: string): void {
    if (!(file instanceof TFile)) return;
    if (shouldIgnore(file.path, this.ignorePaths)) return;
    this.dirtyPaths.add("RENAME:" + oldPath + "→" + file.path);
    this.schedulePush();
  }

  // --- Push logic ---

  private shouldSkip(file: TFile): boolean {
    if (shouldIgnore(file.path, this.ignorePaths)) return true;
    if (file.stat.size > this.maxFileSize) return true;
    return false;
  }

  private markDirty(path: string): void {
    this.dirtyPaths.add(path);
    const entry = this.syncState.get(path);
    if (entry) {
      entry.status = "local_dirty";
    }
    this.schedulePush();
  }

  private schedulePush(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
    }
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      this.processDirty();
    }, this.debounceMs);
  }

  private async processDirty(): Promise<void> {
    if (this.running) return;
    if (this.dirtyPaths.size === 0) return;

    this.running = true;
    const paths = [...this.dirtyPaths];
    this.dirtyPaths.clear();

    const fileCount = paths.filter((p) => !p.includes(":")).length;
    if (fileCount > 0) {
      this.emitStatus("syncing", `${fileCount} file(s)`);
    }

    let errorCount = 0;

    for (const entry of paths) {
      try {
        if (entry.startsWith("DELETE:")) {
          await this.pushDelete(entry.slice(7));
        } else if (entry.startsWith("RENAME:")) {
          const parts = entry.slice(7).split("→");
          if (parts.length === 2) {
            await this.pushRename(parts[0], parts[1]);
          }
        } else {
          await this.pushFile(entry);
        }
      } catch (err) {
        errorCount++;
        console.error(`drive9: sync error for ${entry}:`, err);
      }
    }

    this.running = false;

    if (errorCount > 0) {
      this.emitStatus("error", `${errorCount} error(s)`);
    } else {
      this.emitStatus("idle");
    }
  }

  private async pushFile(path: string): Promise<void> {
    const file = this.vault.getAbstractFileByPath(path);
    if (!(file instanceof TFile)) return;
    if (this.shouldSkip(file)) return;

    const state = this.syncState.get(path);

    // Read file content
    const data = await this.vault.readBinary(file);

    try {
      await this.client.write(
        "/" + path,
        data,
        state?.remoteRevision,
      );

      // Get new revision after write
      const stat = await this.client.stat("/" + path);

      const now = Date.now();
      this.syncState.set(path, {
        path,
        localMtime: file.stat.mtime,
        localSize: data.byteLength,
        remoteRevision: stat?.revision ?? 0,
        syncedAt: now,
        status: "synced",
      });
    } catch (err) {
      if (err instanceof RevisionConflictError) {
        // Mark as conflict, do not overwrite
        const existing = this.syncState.get(path);
        if (existing) {
          existing.status = "conflict";
        } else {
          this.syncState.set(path, {
            path,
            localMtime: file.stat.mtime,
            localSize: data.byteLength,
            remoteRevision: 0,
            syncedAt: 0,
            status: "conflict",
          });
        }
        new Notice(`drive9: Conflict on ${path} — remote was modified.`);
        return;
      }
      throw err;
    }
  }

  private async pushDelete(path: string): Promise<void> {
    try {
      await this.client.delete("/" + path);
      this.syncState.delete(path);
    } catch (err) {
      console.error(`drive9: Failed to delete remote ${path}:`, err);
    }
  }

  private async pushRename(oldPath: string, newPath: string): Promise<void> {
    try {
      await this.client.rename("/" + oldPath, "/" + newPath);
      const oldState = this.syncState.get(oldPath);
      if (oldState) {
        this.syncState.delete(oldPath);
        oldState.path = newPath;
        this.syncState.set(newPath, oldState);
      }
    } catch (err) {
      // Fallback: delete old + push new
      console.warn(
        `drive9: Rename failed, falling back to delete+push for ${oldPath} → ${newPath}`,
      );
      await this.pushDelete(oldPath);
      this.markDirty(newPath);
    }
  }

  stop(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
  }
}
