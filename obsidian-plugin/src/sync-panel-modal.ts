import { App, Modal } from "obsidian";
import { t } from "./i18n";
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

    contentEl.createEl("h2", { text: t("syncPanel.title") });

    // Status section
    const statusEl = contentEl.createEl("div", { cls: "drive9-sync-status" });
    const { status } = this.info;

    if (status === "offline") {
      statusEl.createEl("p", { text: t("syncPanel.offline"), cls: "drive9-status-offline" });
    } else if (status === "error") {
      const detail = this.info.lastErrorDetail;
      statusEl.createEl("p", {
        text: detail ? t("syncPanel.errorDetail", { detail }) : t("syncPanel.error"),
        cls: "drive9-status-error",
      });
    } else if (status === "syncing") {
      const count = this.info.pendingCount;
      statusEl.createEl("p", {
        text: t(count !== 1 ? "syncPanel.syncingPlural" : "syncPanel.syncing", { count }),
        cls: "drive9-status-syncing",
      });
    } else {
      statusEl.createEl("p", { text: t("syncPanel.allSynced"), cls: "drive9-status-ok" });
    }

    // Retry button for error/offline states
    if (status === "error" || status === "offline") {
      const retryBtn = statusEl.createEl("button", { text: t("syncPanel.retryBtn"), cls: "mod-cta" });
      retryBtn.addEventListener("click", () => {
        this.info.onRetry();
        this.close();
      });
    }

    // Conflicts section
    if (this.info.conflicts.length > 0) {
      const conflictSection = contentEl.createEl("div", { cls: "drive9-sync-conflicts" });
      const cLen = this.info.conflicts.length;
      conflictSection.createEl("h3", {
        text: t(cLen > 1 ? "syncPanel.conflictsTitlePlural" : "syncPanel.conflictsTitle", { count: cLen }),
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
      const pCount = this.info.pendingCount;
      contentEl.createEl("p", {
        text: t(pCount !== 1 ? "syncPanel.pendingFilesPlural" : "syncPanel.pendingFiles", { count: pCount }),
        cls: "drive9-sync-pending",
      });
    }

    // Skipped files
    if (this.info.skippedLargeFiles.length > 0) {
      const skippedSection = contentEl.createEl("div", { cls: "drive9-sync-skipped" });
      skippedSection.createEl("h3", {
        text: t("syncPanel.skippedTitle", { count: this.info.skippedLargeFiles.length }),
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
