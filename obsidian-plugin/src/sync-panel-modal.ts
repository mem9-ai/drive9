import { App, Modal } from "obsidian";
import type { SyncStatus } from "./sync-engine";
import type { SyncState } from "./types";

interface SyncPanelInfo {
  status: SyncStatus;
  pendingCount: number;
  lastErrorDetail: string;
  skippedLargeFiles: string[];
  conflicts: Array<{ path: string; state: SyncState }>;
  onRetry: () => void;
  onOpenFile: (path: string) => void;
}

export class SyncPanelModal extends Modal {
  constructor(
    app: App,
    private info: SyncPanelInfo,
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.addClass("drive9-sync-panel");

    contentEl.createEl("h2", { text: "drive9 Sync Status" });

    // Status section
    const statusEl = contentEl.createEl("div", { cls: "drive9-sync-status" });
    const { status } = this.info;

    if (status === "offline") {
      statusEl.createEl("p", { text: "⏸ Offline — server unreachable", cls: "drive9-status-offline" });
    } else if (status === "error") {
      const detail = this.info.lastErrorDetail;
      statusEl.createEl("p", {
        text: detail ? `✗ Error: ${detail} failed to sync` : "✗ Error: sync failed",
        cls: "drive9-status-error",
      });
    } else if (status === "syncing") {
      statusEl.createEl("p", {
        text: `↕ Syncing ${this.info.pendingCount} file${this.info.pendingCount !== 1 ? "s" : ""}...`,
        cls: "drive9-status-syncing",
      });
    } else {
      statusEl.createEl("p", { text: "✓ All files synced", cls: "drive9-status-ok" });
    }

    // Retry button for error/offline states
    if (status === "error" || status === "offline") {
      const retryBtn = statusEl.createEl("button", { text: "Retry Sync", cls: "mod-cta" });
      retryBtn.addEventListener("click", () => {
        this.info.onRetry();
        this.close();
      });
    }

    // Conflicts section
    if (this.info.conflicts.length > 0) {
      const conflictSection = contentEl.createEl("div", { cls: "drive9-sync-conflicts" });
      conflictSection.createEl("h3", {
        text: `${this.info.conflicts.length} Conflict${this.info.conflicts.length > 1 ? "s" : ""}`,
      });

      const list = conflictSection.createEl("ul", { cls: "drive9-conflict-list" });
      for (const { path } of this.info.conflicts) {
        const li = list.createEl("li");
        const link = li.createEl("a", { text: path, cls: "drive9-conflict-link" });
        link.addEventListener("click", (e) => {
          e.preventDefault();
          this.info.onOpenFile(path);
          this.close();
        });
      }
    }

    // Pending files
    if (this.info.pendingCount > 0 && status !== "syncing") {
      contentEl.createEl("p", {
        text: `${this.info.pendingCount} file${this.info.pendingCount !== 1 ? "s" : ""} queued for sync`,
        cls: "drive9-sync-pending",
      });
    }

    // Skipped files
    if (this.info.skippedLargeFiles.length > 0) {
      const skippedSection = contentEl.createEl("div", { cls: "drive9-sync-skipped" });
      skippedSection.createEl("h3", {
        text: `${this.info.skippedLargeFiles.length} Skipped (too large)`,
      });
      const list = skippedSection.createEl("ul");
      for (const path of this.info.skippedLargeFiles) {
        list.createEl("li", { text: path });
      }
    }
  }

  onClose(): void {
    this.contentEl.empty();
  }
}
