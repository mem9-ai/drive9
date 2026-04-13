import type { DataAdapter } from "obsidian";
import type { SyncState } from "./types";

const SHADOW_DIR = ".obsidian/plugins/drive9/shadow";

/**
 * ShadowStore saves a copy of file content at last-sync for use as the
 * merge base in 3-way conflict resolution. Files are stored by content
 * hash (SHA-256 hex) for deduplication.
 */
export class ShadowStore {
  constructor(private adapter: DataAdapter) {}

  async save(data: ArrayBuffer): Promise<string> {
    const hash = await this.hash(data);
    const shadowPath = `${SHADOW_DIR}/${hash}.bin`;
    if (!(await this.exists(shadowPath))) {
      await this.ensureDir();
      await this.adapter.writeBinary(shadowPath, data);
    }
    return hash;
  }

  async load(hash: string): Promise<ArrayBuffer | null> {
    const shadowPath = `${SHADOW_DIR}/${hash}.bin`;
    if (!(await this.exists(shadowPath))) {
      return null;
    }
    return this.adapter.readBinary(shadowPath);
  }

  /**
   * Remove shadow files that are not referenced by any SyncState.
   */
  async gc(syncStates: Record<string, SyncState>): Promise<number> {
    const referencedHashes = new Set<string>();
    for (const state of Object.values(syncStates)) {
      if (state.lastSyncedContentHash) {
        referencedHashes.add(state.lastSyncedContentHash);
      }
    }

    let removed = 0;
    if (!(await this.exists(SHADOW_DIR))) {
      return removed;
    }

    const listing = await this.adapter.list(SHADOW_DIR);
    for (const filePath of listing.files) {
      const name = filePath.split("/").pop() ?? "";
      const hash = name.replace(/\.bin$/, "");
      if (!referencedHashes.has(hash)) {
        try {
          await this.adapter.remove(filePath);
          removed++;
        } catch {
          // Ignore removal failures during GC.
        }
      }
    }
    return removed;
  }

  private async hash(data: ArrayBuffer): Promise<string> {
    const hashBuffer = await crypto.subtle.digest("SHA-256", data);
    const bytes = new Uint8Array(hashBuffer);
    let hex = "";
    for (const b of bytes) {
      hex += b.toString(16).padStart(2, "0");
    }
    return hex;
  }

  private async ensureDir(): Promise<void> {
    if (!(await this.exists(SHADOW_DIR))) {
      await this.adapter.mkdir(SHADOW_DIR);
    }
  }

  private async exists(path: string): Promise<boolean> {
    return this.adapter.exists(path);
  }
}
