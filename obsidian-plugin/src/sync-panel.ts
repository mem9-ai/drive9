import { App, Modal } from "obsidian";
import type { SyncState } from "./types";

export interface SyncPanelInfo {
  syncedCount: number;
  pendingCount: number;
  conflictPaths: string[];
  skippedLargeFiles: string[];
  lastErrorPath: string;
  isError: boolean;
  isOffline: boolean;
}

/**
 * Modal shown when clicking the drive9 status bar.
 * Shows sync summary, conflicts, and a retry button if needed.
 */
export class SyncPanelModal extends Modal {
  constructor(
    app: App,
    private info: SyncPanelInfo,
    private onRetry: () => void,
    private onOpenConflict: (path: string) => void,
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.addClass("drive9-sync-panel");

    contentEl.createEl("h2", { text: "drive9 Sync Status" });

    const summary = contentEl.createDiv({ cls: "drive9-sync-summary" });

    if (this.info.isOffline) {
      summary.createDiv({ text: "Status: offline", cls: "drive9-sync-offline" });
    } else if (this.info.isError) {
      const errorDiv = summary.createDiv({ cls: "drive9-sync-error" });
      errorDiv.createSpan({ text: "Status: error" });
      if (this.info.lastErrorPath) {
        errorDiv.createSpan({
          text: ` — ${this.info.lastErrorPath}`,
          cls: "drive9-sync-error-path",
        });
      }
    } else {
      summary.createDiv({ text: "Status: synced" });
    }

    summary.createDiv({ text: `Synced files: ${this.info.syncedCount}` });

    if (this.info.pendingCount > 0) {
      summary.createDiv({ text: `Pending: ${this.info.pendingCount}` });
    }

    if (this.info.skippedLargeFiles.length > 0) {
      summary.createDiv({
        text: `Skipped (too large): ${this.info.skippedLargeFiles.length}`,
      });
    }

    if (this.info.conflictPaths.length > 0) {
      const conflictSection = contentEl.createDiv({ cls: "drive9-sync-conflicts" });
      conflictSection.createEl("h3", { text: `Conflicts (${this.info.conflictPaths.length})` });
      const list = conflictSection.createEl("ul");
      for (const path of this.info.conflictPaths) {
        const li = list.createEl("li");
        const link = li.createEl("a", { text: path, href: "#" });
        link.addEventListener("click", (e) => {
          e.preventDefault();
          this.close();
          this.onOpenConflict(path);
        });
      }
    }

    const actions = contentEl.createDiv({ cls: "drive9-sync-actions" });

    if (this.info.isError || this.info.pendingCount > 0) {
      const retryBtn = actions.createEl("button", { text: "Retry Sync", cls: "mod-cta" });
      retryBtn.addEventListener("click", () => {
        this.close();
        this.onRetry();
      });
    }

    const closeBtn = actions.createEl("button", { text: "Close" });
    closeBtn.addEventListener("click", () => this.close());
  }

  onClose(): void {
    this.contentEl.empty();
  }
}

export function buildSyncPanelInfo(
  syncStates: Record<string, SyncState>,
  pendingCount: number,
  skippedLargeFiles: string[],
  lastErrorPath: string,
  isError: boolean,
  isOffline: boolean,
): SyncPanelInfo {
  let syncedCount = 0;
  const conflictPaths: string[] = [];

  for (const [path, state] of Object.entries(syncStates)) {
    if (state.status === "synced") syncedCount++;
    if (state.status === "conflict") conflictPaths.push(path);
  }

  return {
    syncedCount,
    pendingCount,
    conflictPaths,
    skippedLargeFiles: [...skippedLargeFiles],
    lastErrorPath,
    isError,
    isOffline,
  };
}
