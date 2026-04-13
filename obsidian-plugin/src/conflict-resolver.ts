import { App, Vault, TFile, Notice } from "obsidian";
import { Drive9Client } from "./client";
import { ShadowStore } from "./shadow-store";
import { merge3 } from "./diff3";
import { ConflictModal, createConflictInfo, isTextFile } from "./conflict-modal";
import type { ConflictChoice } from "./conflict-modal";
import type { SyncState } from "./types";

/**
 * ConflictResolver handles conflict and remote_deleted states
 * produced by the SyncEngine.
 */
export class ConflictResolver {
  private shadowStore: ShadowStore;
  private resolving = false;

  constructor(
    private app: App,
    private vault: Vault,
    private client: Drive9Client,
    private syncStates: Record<string, SyncState>,
    private persistData: () => Promise<void>,
  ) {
    this.shadowStore = new ShadowStore(vault.adapter);
  }

  /**
   * Save a shadow copy of content after a successful sync.
   * Returns the content hash for storing in SyncState.
   */
  async saveShadow(data: ArrayBuffer): Promise<string> {
    return this.shadowStore.save(data);
  }

  /** Run GC on shadow store to clean up unreferenced files. */
  async gcShadows(): Promise<number> {
    return this.shadowStore.gc(this.syncStates);
  }

  /**
   * Scan all SyncState entries and resolve pending conflicts and remote deletes.
   * Only one resolution loop runs at a time.
   */
  async resolveAll(): Promise<void> {
    if (this.resolving) return;
    this.resolving = true;
    try {
      for (const [path, state] of Object.entries(this.syncStates)) {
        switch (state.status) {
          case "conflict":
            await this.resolveConflict(path, state);
            break;
          case "remote_deleted":
            await this.applyRemoteDelete(path, state);
            break;
        }
      }
      await this.persistData();
    } finally {
      this.resolving = false;
    }
  }

  private async resolveConflict(path: string, state: SyncState): Promise<void> {
    const localFile = this.getLocalFile(path);

    if (!localFile) {
      // Local file was deleted — remote wins by default
      delete this.syncStates[path];
      return;
    }

    let remoteData: ArrayBuffer;
    try {
      remoteData = await this.client.read(path);
    } catch {
      console.warn(`[drive9] cannot read remote for conflict resolution: ${path}`);
      return;
    }

    let remoteStat: { revision: number; mtime: number; size: number };
    try {
      const st = await this.client.stat(path);
      remoteStat = { revision: st.revision, mtime: st.mtime, size: st.size };
    } catch {
      remoteStat = { revision: 0, mtime: 0, size: remoteData.byteLength };
    }

    const localData = await this.vault.readBinary(localFile);

    if (isTextFile(path)) {
      const resolved = await this.tryTextMerge(path, state, localData, remoteData, remoteStat);
      if (resolved) return;
    }

    // Auto-merge failed or binary file — show modal
    const info = createConflictInfo(
      path,
      localFile.stat.size,
      localFile.stat.mtime,
      remoteStat.size,
      remoteStat.mtime,
    );
    const choice = await new ConflictModal(this.app, info).open();
    if (choice === null) {
      // User dismissed modal — keep conflict for next cycle
      return;
    }
    await this.applyChoice(path, choice, localFile, localData, remoteData, remoteStat.revision);
  }

  private async tryTextMerge(
    path: string,
    state: SyncState,
    localData: ArrayBuffer,
    remoteData: ArrayBuffer,
    remoteStat: { revision: number; mtime: number; size: number },
  ): Promise<boolean> {
    const localText = decodeText(localData);
    const remoteText = decodeText(remoteData);

    // Load base from shadow store
    let baseText = "";
    if (state.lastSyncedContentHash) {
      const baseData = await this.shadowStore.load(state.lastSyncedContentHash);
      if (baseData) {
        baseText = decodeText(baseData);
      }
    }

    const result = merge3(baseText, localText, remoteText);

    if (!result.hasConflicts) {
      // Auto-merge succeeded — apply merged content
      const mergedData = encodeText(result.merged);
      await this.vault.modifyBinary(this.getLocalFile(path)!, mergedData);

      // Push merged version to remote
      const writeResult = await this.client.write(path, mergedData, state.remoteRevision);

      const updatedFile = this.getLocalFile(path);
      const hash = await this.shadowStore.save(mergedData);
      this.syncStates[path] = {
        path,
        localMtime: updatedFile?.stat.mtime ?? 0,
        localSize: updatedFile?.stat.size ?? 0,
        remoteRevision: writeResult.revision ?? remoteStat.revision,
        syncedAt: Date.now(),
        status: "synced",
        lastSyncedContentHash: hash,
      };
      new Notice(`drive9: auto-merged ${path}`);
      return true;
    }

    return false;
  }

  private async applyChoice(
    path: string,
    choice: ConflictChoice,
    localFile: TFile,
    localData: ArrayBuffer,
    remoteData: ArrayBuffer,
    remoteRevision: number,
  ): Promise<void> {
    switch (choice) {
      case "keep_local": {
        // Overwrite remote with local
        const result = await this.client.write(path, localData);
        const hash = await this.shadowStore.save(localData);
        this.syncStates[path] = {
          path,
          localMtime: localFile.stat.mtime,
          localSize: localFile.stat.size,
          remoteRevision: result.revision ?? remoteRevision,
          syncedAt: Date.now(),
          status: "synced",
          lastSyncedContentHash: hash,
        };
        new Notice(`drive9: kept local version of ${path}`);
        break;
      }

      case "keep_remote": {
        // Overwrite local with remote
        await this.vault.modifyBinary(localFile, remoteData);
        const updatedFile = this.getLocalFile(path);
        const hash = await this.shadowStore.save(remoteData);
        this.syncStates[path] = {
          path,
          localMtime: updatedFile?.stat.mtime ?? 0,
          localSize: updatedFile?.stat.size ?? 0,
          remoteRevision,
          syncedAt: Date.now(),
          status: "synced",
          lastSyncedContentHash: hash,
        };
        new Notice(`drive9: kept remote version of ${path}`);
        break;
      }

      case "keep_both": {
        // Save remote as {name}.conflict.{ext}
        const conflictPath = makeConflictPath(path);
        const dir = conflictPath.includes("/")
          ? conflictPath.slice(0, conflictPath.lastIndexOf("/"))
          : "";
        if (dir && !this.vault.getAbstractFileByPath(dir)) {
          await this.vault.createFolder(dir);
        }
        await this.vault.createBinary(conflictPath, remoteData);

        // Mark local as synced (push it to remote)
        const result = await this.client.write(path, localData);
        const hash = await this.shadowStore.save(localData);
        this.syncStates[path] = {
          path,
          localMtime: localFile.stat.mtime,
          localSize: localFile.stat.size,
          remoteRevision: result.revision ?? remoteRevision,
          syncedAt: Date.now(),
          status: "synced",
          lastSyncedContentHash: hash,
        };
        new Notice(`drive9: kept both versions of ${path}`);
        break;
      }
    }
  }

  private async applyRemoteDelete(path: string, state: SyncState): Promise<void> {
    if (state.deleteDetectionSource === "polling") {
      if ((state.consecutiveAbsences ?? 0) < 2) {
        // Not stable yet — skip this cycle, will re-check next poll
        return;
      }
    }

    const localFile = this.getLocalFile(path);
    if (localFile) {
      // Move to Obsidian trash — never permanent delete
      await this.vault.trash(localFile, true);
    }
    delete this.syncStates[path];
  }

  private getLocalFile(path: string): TFile | null {
    const file = this.vault.getAbstractFileByPath(path);
    return file instanceof TFile ? file : null;
  }
}

function makeConflictPath(path: string): string {
  const lastDot = path.lastIndexOf(".");
  if (lastDot === -1) {
    return `${path}.conflict`;
  }
  return `${path.slice(0, lastDot)}.conflict${path.slice(lastDot)}`;
}

function decodeText(data: ArrayBuffer): string {
  return new TextDecoder().decode(data);
}

function encodeText(text: string): ArrayBuffer {
  return new TextEncoder().encode(text).buffer;
}
