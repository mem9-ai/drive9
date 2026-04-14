import { App, Vault, TFile, Notice } from "obsidian";
import { Drive9Client, Drive9Error } from "./client";
import { ShadowStore } from "./shadow-store";
import { merge3, simpleDiff } from "./diff3";
import { ConflictModal, createConflictInfo, isTextFile } from "./conflict-modal";
import { t } from "./i18n";
import type { ConflictChoice } from "./conflict-modal";
import type { SyncState } from "./types";

/**
 * ConflictResolver handles conflict and remote_deleted states
 * produced by the SyncEngine.
 */
export class ConflictResolver {
  private shadowStore: ShadowStore;
  private resolving = false;
  private suppressLocalEvent: ((path: string, fn: () => Promise<void>) => Promise<void>) | null = null;

  constructor(
    private app: App,
    private vault: Vault,
    private client: Drive9Client,
    private syncStates: Record<string, SyncState>,
    private persistData: () => Promise<void>,
  ) {
    this.shadowStore = new ShadowStore(vault.adapter);
  }

  setSuppressLocalEvent(fn: (path: string, cb: () => Promise<void>) => Promise<void>): void {
    this.suppressLocalEvent = fn;
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

    // Auto-merge failed or binary file — check if user already dismissed this conflict
    // Only use fingerprint when we have a real revision (not the fallback 0)
    const hasRealRevision = remoteStat.revision > 0;
    const fingerprint = hasRealRevision ? `${path}:${remoteStat.revision}` : null;
    if (fingerprint && state.dismissedFingerprint === fingerprint) {
      return;
    }

    let diffPreview: string | undefined;
    if (isTextFile(path)) {
      try {
        const localText = decodeText(localData);
        const remoteText = decodeText(remoteData);
        diffPreview = simpleDiff(localText, remoteText);
      } catch {
        // diff preview is best-effort
      }
    }

    const info = createConflictInfo(
      path,
      localFile.stat.size,
      localFile.stat.mtime,
      remoteStat.size,
      remoteStat.mtime,
      diffPreview,
    );
    const choice = await new ConflictModal(this.app, info).open();
    if (choice === null) {
      // User dismissed modal — only record fingerprint if we have a real revision
      // When revision is unknown, allow re-popup next cycle rather than permanently suppress
      if (fingerprint) {
        state.dismissedFingerprint = fingerprint;
      }
      return;
    }
    state.dismissedFingerprint = undefined;
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
      // Auto-merge succeeded — push to remote first, only apply locally after CAS succeeds
      const mergedData = encodeText(result.merged);

      let writeResult: { revision: number | null; writeSucceeded: boolean };
      try {
        writeResult = await this.client.write(path, mergedData, state.remoteRevision);
      } catch (e) {
        if (e instanceof Drive9Error && e.status === 409) {
          // Remote changed again during merge — stay in conflict for next cycle
          // Do NOT modify local vault; caller will not show modal either
          return true;
        }
        throw e;
      }

      // CAS succeeded — now apply merged content to local vault
      await this.vault.modifyBinary(this.getLocalFile(path)!, mergedData);

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
      new Notice(t("notice.autoMerged", { path }));
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
        // Overwrite remote with local — use CAS to avoid overwriting newer remote versions
        try {
          const result = await this.client.write(path, localData, remoteRevision);
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
          new Notice(t("notice.keptLocal", { path }));
        } catch (e) {
          if (e instanceof Drive9Error && e.status === 409) {
            new Notice(t("notice.remoteChangedRetry", { path }));
            return;
          }
          throw e;
        }
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
        new Notice(t("notice.keptRemote", { path }));
        break;
      }

      case "keep_both": {
        // Save remote as {name}.conflict.{ext} — suppress to prevent push-back
        const conflictPath = makeConflictPath(path);
        const createConflictCopy = async () => {
          const dir = conflictPath.includes("/")
            ? conflictPath.slice(0, conflictPath.lastIndexOf("/"))
            : "";
          if (dir && !this.vault.getAbstractFileByPath(dir)) {
            await this.vault.createFolder(dir);
          }
          await this.vault.createBinary(conflictPath, remoteData);
        };
        if (this.suppressLocalEvent) {
          await this.suppressLocalEvent(conflictPath, createConflictCopy);
        } else {
          await createConflictCopy();
        }

        // Mark local as synced (push it to remote) — use CAS
        try {
          const result = await this.client.write(path, localData, remoteRevision);
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
          new Notice(t("notice.keptBoth", { path }));
        } catch (e) {
          if (e instanceof Drive9Error && e.status === 409) {
            new Notice(t("notice.remoteChangedRetry", { path }));
            return;
          }
          throw e;
        }
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
      // Move to Obsidian .trash — never permanent delete
      await this.vault.trash(localFile, false);
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
